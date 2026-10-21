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
    void subscribe();
    void unsubscribe(bool finish_queue);

    void ackConsumed();
    void dropConsumed();
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

    static void onMsg(natsConnection * nc, natsSubscription * sub, natsMsg * msg, void * consumer);

    virtual void subscribeImpl() = 0;

    virtual void nackMessage(natsMsg * msg);

    virtual bool needsAck() const { return false; }

private:
    std::shared_ptr<ConcurrentBoundedQueue<MessageData>> loadReceived() const;
    void storeReceived(std::shared_ptr<ConcurrentBoundedQueue<MessageData>> queue);

    NATSConnectionPtr connection;
    std::vector<NATSSubscriptionPtr> subscriptions;
    const std::vector<String> subjects;
    LoggerPtr log;
    const std::atomic<bool> & stopped;

    String queue_name;

    const uint32_t queue_size;
    mutable std::mutex received_mutex;
    std::shared_ptr<ConcurrentBoundedQueue<MessageData>> received;
    MessageData current;
    std::vector<NatsMsgPtr> consumed_messages;
};

}
