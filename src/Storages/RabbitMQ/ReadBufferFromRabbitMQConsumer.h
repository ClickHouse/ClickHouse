#pragma once

#include <Core/Names.h>
#include <base/types.h>
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

class ReadBufferFromRabbitMQConsumer : public ReadBuffer
{

public:
    ReadBufferFromRabbitMQConsumer(
            RabbitMQHandler & event_handler_,
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

        AckTracker() = default;
        AckTracker(UInt64 tag, String id) : delivery_tag(tag), channel_id(id) {}
    };

    struct MessageData
    {
        String message;
        String message_id;
        uint64_t timestamp = 0;
        bool redelivered = false;
        AckTracker track{};
    };

    ChannelPtr & getChannel() { return consumer_channel; }
    void setupChannel();
    bool needChannelUpdate();
    void closeChannel()
    {
        if (consumer_channel)
            consumer_channel->close();
    }

    void updateQueues(std::vector<String> & queues_) { queues = queues_; }
    size_t queuesCount() { return queues.size(); }

    bool isConsumerStopped() { return stopped; }
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
    RabbitMQHandler & event_handler; /// Used concurrently, but is thread safe.
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
