#pragma once

#include <memory>
#include <optional>
#include <atomic>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/logger_useful.h>
#include <Core/Types.h>

#include "config.h"

#if USE_AWS_SQS

#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>

namespace DB
{

class SQSConsumer
{
public:
    struct Message
    {
        String data;
        String receipt_handle;
        String message_id;
        UInt64 receive_count = 1;
        UInt64 sent_timestamp = 0; /// Unix timestamp in seconds
        String message_group_id;
        String message_deduplication_id;
        UInt64 sequence_number = 0;
    };

    SQSConsumer(
        const String & queue_url_,
        const Aws::SQS::SQSClient & client_,
        size_t max_messages_per_receive_,
        int visibility_timeout_,
        int wait_time_seconds_,
        const String & dead_letter_queue_url_ = "",
        size_t max_receive_count_ = 3,
        size_t internal_queue_size_ = 100);

    ~SQSConsumer();

    void stop();
    bool isStopped() const { return !is_running; }

    /// Receive messages from SQS and push them to the internal queue.
    /// Returns true if at least one message was received.
    bool receive();

    /// Pop one message from the internal queue (non-blocking).
    std::optional<Message> getMessage();

    /// Delete a message from the SQS queue (acknowledge processing).
    void deleteMessage(const String & receipt_handle);

    /// Move a message to the Dead Letter Queue and delete it from the source queue.
    void moveMessageToDLQ(const Message & message);

    UInt32 getConsumerId() const { return consumer_id; }

private:
    Aws::SQS::Model::ReceiveMessageRequest makeReceiveMessageRequest() const;

    const UInt32 consumer_id;
    const String queue_url;
    const Aws::SQS::SQSClient & client;
    const size_t max_messages_per_receive;
    const int visibility_timeout;
    const int wait_time_seconds;
    const String dead_letter_queue_url;
    const size_t max_receive_count;

    ConcurrentBoundedQueue<Message> queue;
    std::atomic<bool> is_running{true};

    LoggerPtr log;
};

using SQSConsumerPtr = std::shared_ptr<SQSConsumer>;

}

#endif // USE_AWS_SQS
