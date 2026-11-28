#pragma once

#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <Storages/NATS/NATSConnection.h>
#include <Common/DequeWithMemoryTracking.h>
#include <base/types.h>

#include <atomic>
#include <memory>
#include <optional>

namespace DB
{

/// A consumer with its own connection to NATS. Messages are read from the socket only while the consumer is used.
/// A consumer has a single owner at a time and needs no locking: the storage while it is in its pool, or the source
/// that took it out with `popConsumer` until it gives it back with `pushConsumer`.
class INATSConsumer
{
public:
    INATSConsumer(
        const NATSConfiguration & configuration,
        const std::vector<String> & subjects_,
        const String & subscribe_queue_name,
        LoggerPtr log_,
        const std::atomic<bool> & stopped_);
    virtual ~INATSConsumer() = default;

    struct MessageData
    {
        String message;
        String subject;
        /// The subject to acknowledge a JetStream message on. Empty for core NATS, which has no acknowledgements.
        String reply;
    };

    [[nodiscard]] bool isSubscribed() const { return subscribed; }

    /// True when the connection was lost, so that only a re-subscribe, which connects again, resumes consuming.
    bool needsResubscribe() { return subscribed && !connection.isConnected(); }

    /// Connects, if there is no usable connection. Throws `CANNOT_CONNECT_NATS`.
    void connect();

    /// Keeps the connection of a consumer that nobody reads from alive: answers the `PING` of the server,
    /// and connects again if the connection is lost. A subscribed consumer subscribes again then.
    /// Does not throw. Returns whether the consumer is connected, otherwise sets `error`.
    bool maintainConnection(String & error);

    /// Connects if needed and subscribes.
    void subscribe();
    /// Cancels the subscriptions, keeping the connection.
    void unsubscribe();

    /// Closes the connection.
    void disconnect();

    /// What to do with the messages `nats_skip_broken_messages` passed over when they are resolved
    /// before a commit: a streaming cycle never inserts them, so its skip is final (`Acknowledge`),
    /// while a direct `SELECT` consumes nothing until it commits (`ReturnToBroker`).
    enum class SkippedMessages
    {
        Acknowledge,
        ReturnToBroker,
    };

    /// Unsubscribe, and return to the broker every message that was delivered but not committed, so it is redelivered
    /// at once instead of after the ACK deadline.
    void finishAndReturnUnprocessed(SkippedMessages skipped_messages_action);

    void ackConsumed();
    /// Forget the consumed messages; they are redelivered after the ACK deadline.
    void dropConsumed();
    /// Return the consumed messages to the broker, keeping the subscription.
    void returnConsumed();

    /// The message `consume` returned last yielded no rows: it no longer holds back a resubscribe.
    void markLastConsumedSkipped();

    /// True while a consumed message may still turn into rows that nothing has acknowledged yet.
    [[nodiscard]] bool hasConsumedMessages() const { return !consumed_messages.empty(); }

    [[nodiscard]] size_t subjectsCount() const { return subjects.size(); }

    [[nodiscard]] bool isConsumerStopped() const { return stopped; }

    /// Whether no message is available without waiting.
    bool queueEmpty();

    [[nodiscard]] auto getSubject() const { return current.subject; }
    [[nodiscard]] const String & getCurrentMessage() const { return current.message; }

    /// Return read buffer containing next available message or nullptr if there are no messages to
    /// process. With `timeout_ms` set, waits up to that long for a message; without it, returns at once.
    ReadBufferPtr consume(std::optional<UInt64> timeout_ms = std::nullopt);

protected:
    NATSClient::Connection & getConnection() { return connection.get(); }
    [[nodiscard]] const std::vector<String> & getSubjects() const { return subjects; }
    [[nodiscard]] const LoggerPtr & getLogger() const { return log; }
    [[nodiscard]] const String & getQueueName() const { return queue_name; }

    virtual void subscribeImpl() = 0;
    virtual void unsubscribeImpl() = 0;

    /// Waits for the next message of the subscriptions at most `timeout_ms`.
    virtual std::optional<NATSClient::Message> receiveImpl(UInt64 timeout_ms) = 0;

    /// Returns the message if it is a message of the subscriptions of this consumer, which `receiveImpl` returns.
    virtual std::optional<NATSClient::Message> acceptMessage(NATSClient::Message message) = 0;

    [[nodiscard]] virtual bool needsAck() const { return false; }

    /// Before unsubscribing: waits until nothing more is going to be delivered, and returns the reply subjects of what was.
    virtual std::vector<String> drainBeforeUnsubscribe() { return {}; }

private:
    /// Reads the next message into `buffered`, waiting at most `timeout_ms`. Returns whether there is one.
    bool receive(UInt64 timeout_ms);

    /// Keeps the messages that the lost connection has received already: they are inserted as usual, and the
    /// acknowledgements of JetStream messages are sent over the next connection.
    void salvageReceived();
    void addReceived(NATSClient::Message message);

    /// Sends `payload` (an acknowledgement or a negative one) for every message in `messages` and clears it.
    void respond(std::vector<String> & messages, std::string_view payload);

    NATSConnection connection;
    const std::vector<String> subjects;
    LoggerPtr log;
    const std::atomic<bool> & stopped;
    const String queue_name;

    bool subscribed = false;

    DequeWithMemoryTracking<MessageData> buffered;
    MessageData current;
    /// The reply subjects of the consumed JetStream messages.
    std::vector<String> consumed_messages;
    /// Consumed messages that yielded no rows, committed together with `consumed_messages`.
    std::vector<String> skipped_messages;
};

}
