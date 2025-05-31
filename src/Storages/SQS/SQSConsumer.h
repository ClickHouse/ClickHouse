#pragma once

#include <memory>
#include <Common/ConcurrentBoundedQueue.h>
#include <Core/Types.h>
#include <Core/Block.h>
#include <Poco/Logger.h>

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
        UInt64 receive_count{1};
        DateTime64 sent_timestamp{0};
        String message_group_id;
        String message_deduplication_id;
        UInt64 sequence_number{0};
        String attributes;
    };

    SQSConsumer(
        const String & queue_url_,
        const Aws::SQS::SQSClient & client_,
        size_t max_messages_per_receive_,
        int visibility_timeout_,
        int wait_time_seconds_,
        const String & dead_letter_queue_url_ = "",
        size_t max_receive_count_ = 10,
        size_t internal_queue_size_ = 100);

    ~SQSConsumer();

    void stop();
    bool isStopped() const { return !is_running; }

    bool receive();
    std::optional<Message> getMessage();
    void deleteMessage(const String & receipt_handle);
    void moveMessageToDLQ(const Message & message);

    UInt32 consumer_id; // For debug
private:

    Aws::SQS::Model::ReceiveMessageRequest makeReceiveMessageRequest();

    String queue_url;
    
    const Aws::SQS::SQSClient & client;
    
    size_t max_messages_per_receive;
    int visibility_timeout;
    int wait_time_seconds;
    // Parameters for DLQ
    String dead_letter_queue_url;
    size_t max_receive_count;
    
    // Queue for storing received messages
    ConcurrentBoundedQueue<Message> queue;
    std::atomic<bool> is_running;
};

using SQSConsumerPtr = std::shared_ptr<SQSConsumer>;

}

#endif // USE_AWS_SQS
