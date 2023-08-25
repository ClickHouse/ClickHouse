#pragma once

#include <Core/Names.h>
#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <Common/ConcurrentBoundedQueue.h>

namespace Poco
{
class Logger;
}

namespace AMQP
{
class TcpChannel;
}

namespace DB
{

class RabbitMQHandler;
using ChannelPtr = std::unique_ptr<AMQP::TcpChannel>;
static constexpr auto SANITY_TIMEOUT = 1000 * 60 * 10; /// 10min.

class RabbitMQConsumer
{

public:
    RabbitMQConsumer(
            RabbitMQHandler & event_handler_,
            std::vector<String> & queues_,
            size_t channel_id_base_,
            const String & channel_base_,
            Poco::Logger * log_,
            uint32_t queue_size_);

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

    /// Return read buffer containing next available message
    /// or nullptr if there are no messages to process.
    ReadBufferPtr consume();

    ChannelPtr & getChannel() { return consumer_channel; }
    void setupChannel();
    bool needChannelUpdate();
    void shutdown();

    void updateQueues(std::vector<String> & queues_) { queues = queues_; }
    size_t queuesCount() { return queues.size(); }

    bool isConsumerStopped() const { return stopped.load(); }
    bool ackMessages();
    void updateAckTracker(AckTracker record = AckTracker());

    bool hasPendingMessages() { return !received.empty(); }

    auto getChannelID() const { return current.track.channel_id; }
    auto getDeliveryTag() const { return current.track.delivery_tag; }
    auto getRedelivered() const { return current.redelivered; }
    auto getMessageID() const { return current.message_id; }
    auto getTimestamp() const { return current.timestamp; }

    void waitForMessages(std::optional<uint64_t> timeout_ms = std::nullopt)
    {
        std::unique_lock lock(mutex);
        if (!timeout_ms)
            timeout_ms = SANITY_TIMEOUT;
        cv.wait_for(lock, std::chrono::milliseconds(*timeout_ms), [this]{ return !received.empty() || isConsumerStopped(); });
    }

    void closeConnections();

private:
    void subscribe();
    void iterateEventLoop();

    ChannelPtr consumer_channel;
    RabbitMQHandler & event_handler; /// Used concurrently, but is thread safe.
    std::vector<String> queues;
    const String channel_base;
    const size_t channel_id_base;
    Poco::Logger * log;
    std::atomic<bool> stopped;

    String channel_id;
    std::atomic<bool> channel_error = true, wait_subscription = false;
    ConcurrentBoundedQueue<MessageData> received;
    MessageData current;
    size_t subscribed = 0;

    AckTracker last_inserted_record_info;
    UInt64 prev_tag = 0, channel_id_counter = 0;

    std::condition_variable cv;
    std::mutex mutex;
};

}
