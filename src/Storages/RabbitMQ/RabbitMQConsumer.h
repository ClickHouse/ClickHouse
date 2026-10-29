#pragma once

#include <Core/Names.h>
#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/saturatedDuration.h>

#include <condition_variable>
#include <functional>
#include <mutex>

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
using LoggerPtr = std::shared_ptr<Poco::Logger>;

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
    /// Messages read and not acked, also by earlier readers of this consumer, use up the prefetch window. Conservative:
    /// individually rejected messages and several queues per channel only make it true earlier.
    bool isWindowFull() const;
    /// Earlier readers (a direct SELECT) hold the whole window, and acking the last message read settles it.
    bool isWindowHeld() const;
    UInt16 getPrefetchCount() const { return prefetch_count; }

    /// Return read buffer containing next available message
    /// or nullptr if there are no messages to process.
    ReadBufferPtr consume();

    bool needChannelUpdate();
    void updateChannel(RabbitMQConnection & connection);

    void stop();
    bool isConsumerStopped() const { return stopped.load(); }

    bool ackMessages(const CommitInfo & commit_info);
    bool nackMessages(const CommitInfo & commit_info, bool requeue = false);

    bool hasPendingMessages() { return !received.empty(); }

    void waitForMessages(std::optional<uint64_t> timeout_ms = std::nullopt, std::function<bool()> is_cancelled = {})
    {
        std::unique_lock lock(mutex);
        if (!timeout_ms)
            timeout_ms = SANITY_TIMEOUT;
        cv.wait_for(lock, saturatedMilliseconds(*timeout_ms),
            [&]{ return !received.empty() || isConsumerStopped() || (is_cancelled && is_cancelled()); });
    }

    /// Wake a source parked in `waitForMessages` so it can re-check its cancellation state.
    void wakeUp();

    void closeConnections();

    /// True once the broker has answered the channel.close sent by closeConnections(). A consumer
    /// that never had a channel reports true, as there is nothing to wait for.
    bool isChannelCloseCompleted() const { return !close_state || close_state->completed; }

    /// Stop expecting that answer, so a callback dispatched later becomes a no-op.
    void abandonChannelClose() { if (close_state) close_state->abandoned = true; }

private:
    void subscribe(UInt64 generation);
    bool isCurrentChannel(UInt64 generation) const { return generation == channel_generation; }
    bool isChannelUsable();
    void notifyReceivedSpace();
    void updateCommitInfo(CommitInfo record);

    /// Shared by value into the close() callbacks, mirroring RabbitMQProducer::finish, so that a
    /// late dispatch after the wait was abandoned cannot touch a destroyed consumer.
    struct ChannelCloseState
    {
        std::atomic<bool> completed = false;
        std::atomic<bool> abandoned = false;
    };
    std::shared_ptr<ChannelCloseState> close_state;

    ChannelPtr consumer_channel;
    RabbitMQHandler & event_handler; /// Used concurrently, but is thread safe.

    const std::vector<String> queues;
    const String channel_base;
    const size_t channel_id_base;
    /// basic.qos per queue subscription. It bounds `received`, which must never block the event loop thread:
    /// the streaming thread waits for that loop to stop, see https://github.com/ClickHouse/ClickHouse/issues/54775.
    const UInt16 prefetch_count;

    LoggerPtr log;
    std::atomic<bool> stopped;

    String channel_id;
    UInt64 channel_id_counter = 0;
    /// Incremented on every channel update and captured by value in the channel callbacks.
    /// AMQP-CPP can dispatch callbacks of a replaced channel later, they must not touch the consumer state.
    std::atomic<UInt64> channel_generation = 0;

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
    UInt64 last_rejected_delivery_tag = 0;

    std::condition_variable cv;
    std::mutex mutex;

    /// Held by the channel callbacks while they touch the consumer state and by `updateChannel`
    /// while it replaces the generation and clears `received`. Recursive because AMQP-CPP calls the error
    /// callback of a failed `consume` right away, from `subscribe` inside the `onReady` callback.
    std::recursive_mutex channel_mutex;
    /// Notified when `received` gets space, the channel is replaced, or the consumer is stopped.
    std::condition_variable_any received_space_cv;
};

}
