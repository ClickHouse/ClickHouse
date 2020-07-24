#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Common/ConcurrentBoundedQueue.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

using ChannelPtr = std::shared_ptr<AMQP::TcpChannel>;
using HandlerPtr = std::shared_ptr<RabbitMQHandler>;

class ReadBufferFromRabbitMQConsumer : public ReadBuffer
{

public:
    ReadBufferFromRabbitMQConsumer(
            ChannelPtr consumer_channel_,
            ChannelPtr setup_channel_,
            HandlerPtr event_handler_,
            const String & exchange_name_,
            const AMQP::ExchangeType & exchange_type_,
            const Names & routing_keys_,
            size_t channel_id_,
            const String & queue_base_,
            Poco::Logger * log_,
            char row_delimiter_,
            bool hash_exchange_,
            size_t num_queues_,
            const String & local_exchange_,
            const String & deadletter_exchange_,
            const std::atomic<bool> & stopped_);

    ~ReadBufferFromRabbitMQConsumer() override;

    struct MessageData
    {
        UInt64 delivery_tag;
        String message;
        bool redelivered;
    };

    void allowNext() { allowed = true; } // Allow to read next message.
    void checkSubscription();
    void ackMessages(UInt64 last_inserted_delivery_tag);

    auto getConsumerTag() const { return consumer_tag; }
    auto getDeliveryTag() const { return current.delivery_tag; }
    auto getRedelivered() const { return current.redelivered; }

private:
    ChannelPtr consumer_channel;
    ChannelPtr setup_channel;
    HandlerPtr event_handler;

    const String exchange_name;
    const AMQP::ExchangeType exchange_type;
    const Names routing_keys;
    const size_t channel_id;
    const String queue_base;
    const bool hash_exchange;
    const size_t num_queues;

    Poco::Logger * log;
    char row_delimiter;
    bool allowed = true;
    const std::atomic<bool> & stopped;

    const String local_exchange, deadletter_exchange;
    std::atomic<bool> consumer_error = false;
    std::atomic<size_t> count_subscribed = 0, wait_subscribed;

    String consumer_tag;
    ConcurrentBoundedQueue<MessageData> received;
    UInt64 prev_tag = 0;
    MessageData current;
    std::vector<String> queues;
    std::unordered_map<String, bool> subscribed_queue;
    std::atomic<bool> ack = false;
    std::mutex wait_ack;
    UInt64 max_tag = 0;

    bool nextImpl() override;

    void initQueueBindings(const size_t queue_id);
    void subscribe(const String & queue_name);
    void iterateEventLoop();

};
}
