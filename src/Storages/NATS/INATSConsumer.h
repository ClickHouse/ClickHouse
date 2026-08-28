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

    /// True when the NATS client has closed a subscription we still hold: consuming has stopped
    /// and only a re-subscribe resumes it. Base implementation always returns false, because
    /// recovery parts with the buffered messages and only JetStream redelivers them.
    virtual bool needsResubscribe() const { return false; }

    void subscribe();
    void unsubscribe();

    /// Stop buffering and hand back to the broker every message the client has delivered but
    /// ClickHouse has not inserted yet, so JetStream redelivers it at once instead of after the
    /// ACK deadline. Must run before the subscription those messages arrived on is destroyed: a
    /// `natsMsg` keeps a plain pointer to it, and `natsMsg_Nak` follows that pointer to reach the
    /// JetStream context and the connection.
    /// A consumer that is not subscribed holds only leftovers of a subscription that is already
    /// gone, which it can only destroy. That tells the two apart because nothing re-subscribes
    /// without clearing the queue first, so a subscribed consumer never holds a message that
    /// arrived on an older subscription.
    void finishAndReturnUnprocessed();

    void ackConsumed();
    void dropConsumed();

    /// Move the message `consume` returned last out of the set of messages that still owe rows:
    /// it was parsed into none, which `nats_skip_broken_messages` makes an ordinary outcome rather
    /// than an error. Such a message is never going to produce a row, so it is acknowledged rather
    /// than handed back when the subscription it arrived on has to be replaced - returning it would
    /// undo the skip and show the same malformed input again.
    void markLastConsumedSkipped();

    /// True while this consumer holds messages it has handed out and which may still turn into rows
    /// nothing has acknowledged yet. A message enters this set as soon as `consume` returns it,
    /// before it is parsed, and leaves it again through `markLastConsumedSkipped` once it turns out
    /// to have yielded no rows.
    bool hasConsumedMessages() const { return !consumed_messages.empty(); }

    /// Throw away leftovers of a subscription that is already gone, which is all that can be done
    /// with them: acknowledging or returning a message needs the subscription it arrived on.
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

    /// True if the client has closed any subscription we hold. An empty vector reads as false:
    /// nothing is subscribed, so there is nothing to recover.
    bool hasClosedSubscription() const;

    /// True if the connection has been re-established since we subscribed. The subscriptions we hold
    /// are still valid, but the broker kept nothing of what they were waiting for.
    bool hasConnectionReconnected() const;

    static void onMsg(natsConnection * nc, natsSubscription * sub, natsMsg * msg, void * consumer);

    virtual void subscribeImpl() = 0;

    virtual void nackMessage(natsMsg * msg);

    virtual bool needsAck() const { return false; }

private:
    /// Acknowledge every message in `messages` and clear it.
    void ackMessages(std::vector<NatsMsgPtr> & messages);

    std::shared_ptr<ConcurrentBoundedQueue<MessageData>> loadReceived() const;
    void storeReceived(std::shared_ptr<ConcurrentBoundedQueue<MessageData>> queue);

    NATSConnectionPtr connection;
    std::vector<NATSSubscriptionPtr> subscriptions;
    /// Reconnect count of the connection as of the moment we subscribed. Only ever touched under the
    /// storage's consumers mutex, together with `subscriptions`.
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
    /// Messages that were consumed and parsed into no rows. They are acknowledged together with
    /// `consumed_messages`, but they are never handed back to the broker: nothing is waiting for
    /// them to be inserted.
    std::vector<NatsMsgPtr> skipped_messages;
};

}
