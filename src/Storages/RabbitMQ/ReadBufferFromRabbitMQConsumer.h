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
            size_t channel_id_,
            const String & queue_base_,
            Poco::Logger * log_,
            char row_delimiter_,
            bool hash_exchange_,
            size_t num_queues_,
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
    bool channelUsable() { return !channel_error.load(); }
    void restoreChannel(ChannelPtr new_channel);
    void updateNextDeliveryTag(UInt64 delivery_tag) { last_inserted_delivery_tag = delivery_tag; }
    void ackMessages();

    auto getConsumerTag() const { return consumer_tag; }
    auto getDeliveryTag() const { return current.delivery_tag; }
    auto getRedelivered() const { return current.redelivered; }

private:
    ChannelPtr consumer_channel;
    ChannelPtr setup_channel;
    HandlerPtr event_handler;

    const String exchange_name;
    const size_t channel_id;
    const String queue_base;
    const bool hash_exchange;
    const size_t num_queues;

    Poco::Logger * log;
    char row_delimiter;
    bool allowed = true;
    const std::atomic<bool> & stopped;

    const String deadletter_exchange;
    std::atomic<bool> channel_error = false;

    String consumer_tag;
    ConcurrentBoundedQueue<MessageData> received;
    UInt64 last_inserted_delivery_tag = 0, prev_tag = 0;
    MessageData current;
    std::vector<String> queues;

    bool nextImpl() override;

    void bindQueue(size_t queue_id);
    void subscribe();
    void iterateEventLoop();
};

}
