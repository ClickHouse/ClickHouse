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
class RabbitMQConnection;
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
        LoggerPtr log_,
        uint32_t queue_size_);

    struct CommitInfo
    {
        UInt64 delivery_tag = 0;
        String channel_id;
        std::vector<UInt64> failed_delivery_tags;
    };

    struct MessageData
    {
        String message;
        String message_id;
        UInt64 timestamp = 0;
        bool redelivered = false;
        UInt64 delivery_tag = 0;
        String channel_id;
    };

    const MessageData & currentMessage() { return current; }
    const String & getChannelID() const { return channel_id; }

    /// Return read buffer containing next available message
    /// or nullptr if there are no messages to process.
    ReadBufferPtr consume();

    bool needChannelUpdate();
    void updateChannel(RabbitMQConnection & connection);

    void stop();
    bool isConsumerStopped() const { return stopped.load(); }

    bool ackMessages(const CommitInfo & commit_info);
    bool nackMessages(const CommitInfo & commit_info);

    bool hasPendingMessages() { return !received.empty(); }

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
    bool isChannelUsable();
    void updateCommitInfo(CommitInfo record);

    ChannelPtr consumer_channel;
    RabbitMQHandler & event_handler; /// Used concurrently, but is thread safe.

    const std::vector<String> queues;
    const String channel_base;
    const size_t channel_id_base;

    LoggerPtr log;
    std::atomic<bool> stopped;

    String channel_id;
    UInt64 channel_id_counter = 0;

    enum class State : uint8_t
    {
        NONE,
        INITIALIZING,
        OK,
        ERROR,
    };
    std::atomic<State> state = State::NONE;
    size_t subscriptions_num = 0;

    ConcurrentBoundedQueue<MessageData> received;
    MessageData current;

    UInt64 last_commited_delivery_tag = 0;

    std::condition_variable cv;
    std::mutex mutex;
};

}
