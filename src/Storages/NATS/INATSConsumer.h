#pragma once

#include <nats.h>
#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <Storages/NATS/NATSConnection.h>
#include <base/types.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Storages/NATS/StorageNATS.h>

#include <memory>
#include <mutex>
#include <optional>

namespace Poco
{
class Logger;
}

namespace DB
{

using NATSSubscriptionPtr = std::unique_ptr<natsSubscription, decltype(&natsSubscription_Destroy)>;
using NatsMsgPtr = std::unique_ptr<natsMsg, decltype(&natsMsg_Destroy)>;

/// Apart from the queue of received messages, which the NATS client thread fills, a consumer has a
/// single owner at a time and needs no locking: the storage while it is in its pool, or the source
/// that took it out with `popConsumer` until it gives it back with `pushConsumer`.
class INATSConsumer
{
public:
    INATSConsumer(
        NATSConnectionPtr connection_,
        const std::vector<String> & subjects_,
        const String & subscribe_queue_name,
        LoggerPtr log_,
        uint32_t queue_size_,
        const std::atomic<bool> & stopped_);
    virtual ~INATSConsumer() = default;

    struct MessageData
    {
        String message;
        String subject;
        /// Only kept for JetStream, null for core NATS, which has no ack.
        NatsMsgPtr msg{nullptr, &natsMsg_Destroy};
    };

    bool isSubscribed() const;

    /// True when the subscription stopped consuming and only a re-subscribe resumes it.
    /// Only JetStream redelivers what a re-subscribe hands back, so the base class never asks for one.
    virtual bool needsResubscribe() const { return false; }

    void subscribe();
    void unsubscribe();

    /// What to do with the messages `nats_skip_broken_messages` passed over when they are resolved
    /// before a commit: a streaming cycle never inserts them, so its skip is final (`Acknowledge`),
    /// while a direct `SELECT` consumes nothing until it commits (`ReturnToBroker`).
    enum class SkippedMessages
    {
        Acknowledge,
        ReturnToBroker,
    };

    /// Stop buffering and return to the broker every message that was delivered but not committed,
    /// so it is redelivered at once instead of after the ACK deadline. Must run while the
    /// subscription is alive: `natsMsg_Nak` reaches the connection through it.
    void finishAndReturnUnprocessed(SkippedMessages skipped_messages_action);

    void ackConsumed();
    /// Release the handles of the consumed messages; they are redelivered after the ACK deadline.
    void dropConsumed();
    /// Return the consumed messages to the broker, keeping the subscription and the local queue.
    void returnConsumed();

    /// The message `consume` returned last yielded no rows: it no longer holds back a resubscribe.
    void markLastConsumedSkipped();

    /// True while a consumed message may still turn into rows that nothing has acknowledged yet.
    bool hasConsumedMessages() const { return !consumed_messages.empty(); }

    /// Throw away leftovers of a subscription that is already gone.
    void dropBuffered();

    size_t subjectsCount() { return subjects.size(); }

    bool isConsumerStopped() { return stopped; }

    bool queueEmpty() { return loadReceived()->empty(); }
    size_t queueSize() { return loadReceived()->size(); }

    auto getSubject() const { return current.subject; }
    const String & getCurrentMessage() const { return current.message; }

    /// Return read buffer containing next available message or nullptr if there are no messages to
    /// process. With `timeout_ms` set, waits up to that long for a message; without it, returns at once.
    ReadBufferPtr consume(std::optional<UInt64> timeout_ms = std::nullopt);

protected:
    const NATSConnectionPtr & getConnection() { return connection; }
    natsConnection * getNativeConnection() { return connection->getConnection(); }

    const std::vector<String> & getSubjects() const { return subjects; }
    const LoggerPtr & getLogger() const { return log; }

    const String & getQueueName() const { return queue_name; }

    void setSubscriptions(std::vector<NATSSubscriptionPtr> subscriptions_) { subscriptions = std::move(subscriptions_); }

    /// True if the client has closed any subscription we hold.
    bool hasClosedSubscription() const;

    /// True if the connection has been re-established since we subscribed: the broker has lost the
    /// pull requests of our subscriptions.
    bool hasConnectionReconnected() const;

    bool isConnectionConnected() const;

    static void onMsg(natsConnection * nc, natsSubscription * sub, natsMsg * msg, void * consumer);

    virtual void subscribeImpl() = 0;

    virtual void nackMessage(natsMsg * msg);

    virtual bool needsAck() const { return false; }

private:
    /// Acknowledge or return every message in `messages` and clear it.
    void ackMessages(std::vector<NatsMsgPtr> & messages);
    void nackMessages(std::vector<NatsMsgPtr> & messages);

    std::shared_ptr<ConcurrentBoundedQueue<MessageData>> loadReceived() const;
    void storeReceived(std::shared_ptr<ConcurrentBoundedQueue<MessageData>> queue);

    NATSConnectionPtr connection;
    std::vector<NATSSubscriptionPtr> subscriptions;
    /// Reconnect count of the connection as of the moment we subscribed.
    UInt64 connection_reconnect_count = 0;
    const std::vector<String> subjects;
    LoggerPtr log;
    const std::atomic<bool> & stopped;

    String queue_name;

    const uint32_t queue_size;
    mutable std::mutex received_mutex;
    std::shared_ptr<ConcurrentBoundedQueue<MessageData>> received;
    MessageData current;
    std::vector<NatsMsgPtr> consumed_messages;
    /// Consumed messages that yielded no rows, committed together with `consumed_messages`.
    std::vector<NatsMsgPtr> skipped_messages;
};

}
