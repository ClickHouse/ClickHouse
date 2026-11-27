#pragma once

#include <Core/Names.h>
#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <Storages/RabbitMQ/RabbitMQConnection.h>

#include <atomic>
#include <functional>
#include <optional>


namespace DB
{

static constexpr auto SANITY_TIMEOUT = 1000 * 60 * 10; /// 10min.

/// A consumer of the table's queues with its own connection and channel. Deliveries are read from the socket
/// only while the consumer is used: by a source, which owns the consumer while it reads, or to send heartbeats.
/// Only `stop` can be called concurrently with the other methods.
class RabbitMQConsumer
{
public:
    RabbitMQConsumer(
        const RabbitMQConfiguration & configuration_,
        const std::vector<String> & queues_,
        size_t channel_id_base_,
        const String & channel_base_,
        LoggerPtr log_);

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

    /// Whether the connection or the channel failed, so `updateChannel` is needed to read and commit messages.
    bool needChannelUpdate() const { return state != State::OK; }

    /// Connects again and subscribes to the queues. Does not throw: if it fails, the consumer stays in the error state.
    /// Deliveries of the previous channel can not be acknowledged anymore, and the broker delivers them again.
    void updateChannel();

    void stop();
    bool isConsumerStopped() const { return stopped.load(); }

    bool ackMessages(const CommitInfo & commit_info);
    bool nackMessages(const CommitInfo & commit_info, bool requeue = false);

    /// Whether a message is available without waiting: received before or already in the socket.
    bool hasPendingMessages();

    /// Waits until a message is available, the timeout passes, the consumer is stopped, or `is_cancelled` returns true.
    void waitForMessages(std::optional<uint64_t> timeout_ms = std::nullopt, std::function<bool()> is_cancelled = {});

    /// Sends a heartbeat if it is due, so that the broker does not close the connection of an idle consumer.
    void heartbeat();

    /// Closes the connection, which cancels the subscriptions and returns unacknowledged messages to the queues.
    void closeConnections();

private:
    /// Reads the next delivery into `pending`, waiting at most `timeout_ms`. Returns whether there is one.
    bool receive(UInt64 timeout_ms);

    void setError(const String & what);

    RabbitMQConnection connection;

    const std::vector<String> queues;
    const String channel_base;
    const size_t channel_id_base;

    LoggerPtr log;
    std::atomic<bool> stopped = false;

    String channel_id;
    UInt64 channel_id_counter = 0;

    enum class State : uint8_t
    {
        NONE,
        OK,
        ERROR,
    };
    State state = State::NONE;

    std::optional<AMQP::Delivery> pending;
    MessageData current;

    UInt64 last_commited_delivery_tag = 0;
};

}
