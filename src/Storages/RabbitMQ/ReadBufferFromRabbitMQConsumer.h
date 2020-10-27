#pragma once

#include <Core/Names.h>
#include <common/types.h>
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
            HandlerPtr event_handler_,
            std::vector<String> & queues_,
            size_t channel_id_base_,
            const String & channel_base_,
            Poco::Logger * log_,
            char row_delimiter_,
            uint32_t queue_size_,
            const std::atomic<bool> & stopped_);

    ~ReadBufferFromRabbitMQConsumer() override;

    struct AckTracker
    {
        UInt64 delivery_tag;
        String channel_id;

        AckTracker() : delivery_tag(0), channel_id("") {}
        AckTracker(UInt64 tag, String id) : delivery_tag(tag), channel_id(id) {}
    };

    struct MessageData
    {
        String message;
        String message_id;
        uint64_t timestamp;
        bool redelivered;
        AckTracker track;
    };

    bool isConsumerStopped() { return stopped; }
    bool isChannelError() { return channel_error; }
    /// Do not allow to update channel if current channel is not properly set up and subscribed
    bool isChannelUpdateAllowed() { return !wait_subscription; }

    ChannelPtr & getChannel() { return consumer_channel; }
    void setupChannel();

    bool ackMessages();
    void updateAckTracker(AckTracker record = AckTracker());

    bool queueEmpty() { return received.empty(); }
    void allowNext() { allowed = true; } // Allow to read next message.

    auto getChannelID() const { return current.track.channel_id; }
    auto getDeliveryTag() const { return current.track.delivery_tag; }
    auto getRedelivered() const { return current.redelivered; }
    auto getMessageID() const { return current.message_id; }
    auto getTimestamp() const { return current.timestamp; }

private:
    bool nextImpl() override;

    void subscribe();
    void iterateEventLoop();

    ChannelPtr consumer_channel;
    HandlerPtr event_handler;
    std::vector<String> queues;
    const String channel_base;
    const size_t channel_id_base;
    Poco::Logger * log;
    char row_delimiter;
    bool allowed = true;
    const std::atomic<bool> & stopped;

    String channel_id;
    std::atomic<bool> channel_error = true, wait_subscription = false;
    ConcurrentBoundedQueue<MessageData> received;
    MessageData current;
    size_t subscribed = 0;

    AckTracker last_inserted_record_info;
    UInt64 prev_tag = 0, channel_id_counter = 0;
};

}
