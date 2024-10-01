/*
 * Copyright 2023 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.sqs.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.lang.NonNull;

import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.sqs.eventqueue.SQSObservableQueue;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.common.collect.Maps;

public class SQSQueueDAO implements QueueDAO {

    private final AmazonSQS sqsClient;

    private final Map<String, SQSObservableQueue> queues;

    public SQSQueueDAO(AmazonSQS sqsClient) {
        this.sqsClient = sqsClient;
        this.queues = Maps.newHashMap();
    }

    @NonNull
    public ObservableQueue getQueue(String queueURI) {
        return queues.computeIfAbsent(
                queueURI,
                q ->
                        new SQSObservableQueue.Builder()
                                .withClient(sqsClient)
                                .withQueueName(queueURI)
                                .build());
    }

    private String getQueueUrl(String queueName) {
        return getQueue(queueName).getURI();
    }

    @Override
    public void push(String queueName, String id, long offsetTimeInSecond) {
        SendMessageRequest request =
                new SendMessageRequest()
                        .withQueueUrl(getQueueUrl(queueName))
                        .withDelaySeconds((int) offsetTimeInSecond);
        sqsClient.sendMessage(request);
    }

    @Override
    public void push(String queueName, String id, int priority, long offsetTimeInSecond) {
        push(queueName, id, offsetTimeInSecond); // SQS doesn't support priority natively
    }

    @Override
    public void push(String queueName, List<Message> messages) {
        String queueUrl = getQueueUrl(queueName);
        messages.forEach(
                message -> {
                    SendMessageRequest request =
                            new SendMessageRequest()
                                    .withQueueUrl(queueUrl)
                                    .withMessageBody(message.getPayload());
                    sqsClient.sendMessage(request);
                });
    }

    @Override
    public boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond) {
        if (!containsMessage(queueName, id)) {
            push(queueName, id, offsetTimeInSecond);
            return true;
        }
        return false;
    }

    @Override
    public boolean pushIfNotExists(
            String queueName, String id, int priority, long offsetTimeInSecond) {
        return pushIfNotExists(queueName, id, offsetTimeInSecond); // No priority support
    }

    @Override
    public List<String> pop(String queueName, int count, int timeout) {
        ReceiveMessageRequest request =
                new ReceiveMessageRequest()
                        .withQueueUrl(getQueueUrl(queueName))
                        .withMaxNumberOfMessages(count)
                        .withWaitTimeSeconds(timeout / 1000);
        List<com.amazonaws.services.sqs.model.Message> messages =
                sqsClient.receiveMessage(request).getMessages();
        return messages.stream()
                .map(com.amazonaws.services.sqs.model.Message::getMessageId)
                .collect(Collectors.toList());
    }

    @Override
    public List<Message> pollMessages(String queueName, int count, int timeout) {
        ReceiveMessageRequest request =
                new ReceiveMessageRequest()
                        .withQueueUrl(getQueueUrl(queueName))
                        .withMaxNumberOfMessages(count)
                        .withWaitTimeSeconds(timeout / 1000);
        List<com.amazonaws.services.sqs.model.Message> messages =
                sqsClient.receiveMessage(request).getMessages();
        return messages.stream()
                .map(message -> new Message(message.getMessageId(), message.getBody(), null))
                .collect(Collectors.toList());
    }

    @Override
    public void remove(String queueName, String messageId) {
        String queueUrl = getQueueUrl(queueName);
        ReceiveMessageRequest receiveRequest =
                new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10);
        List<com.amazonaws.services.sqs.model.Message> messages =
                sqsClient.receiveMessage(receiveRequest).getMessages();

        messages.stream()
                .filter(m -> m.getBody().equals(messageId))
                .findFirst()
                .ifPresent(m -> sqsClient.deleteMessage(queueUrl, m.getReceiptHandle()));
    }

    @Override
    public int getSize(String queueName) {
        GetQueueAttributesRequest request =
                new GetQueueAttributesRequest()
                        .withQueueUrl(getQueueUrl(queueName))
                        .withAttributeNames("ApproximateNumberOfMessages");
        return Integer.parseInt(
                sqsClient
                        .getQueueAttributes(request)
                        .getAttributes()
                        .get("ApproximateNumberOfMessages"));
    }

    @Override
    public boolean ack(String queueName, String messageId) {
        remove(queueName, messageId);
        return true;
    }

    @Override
    public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
        // SQS doesn't support modifying visibility timeout by messageId directly.
        return false;
    }

    @Override
    public void flush(String queueName) {
        String queueUrl = getQueueUrl(queueName);
        sqsClient.purgeQueue(new PurgeQueueRequest(queueUrl));
    }

    @Override
    public Map<String, Long> queuesDetail() {
        Map<String, Long> queueDetails =
                queues.keySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Function.identity(), (queue) -> (long) getSize(queue)));
        return queueDetails;
    }

    @Override
    public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
        return new HashMap<>();
    }

    @Override
    public boolean resetOffsetTime(String queueName, String id) {
        // SQS doesn't allow resetting visibility timeout for a specific message without first
        // receiving it.
        return false;
    }

    @Override
    public boolean containsMessage(String queueName, String messageId) {
        List<Message> messages = pollMessages(queueName, 10, 2000); // Quick poll
        return messages.stream().anyMatch(m -> m.getId().equals(messageId));
    }
}
