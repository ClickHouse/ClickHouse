#pragma once

#include <nats.h>
#include <Core/Names.h>
#include <IO/ReadBuffer.h>
#include <Storages/NATS/NATSConnection.h>
#include <base/types.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Storages/NATS/StorageNATS.h>

namespace Poco
{
class Logger;
}

namespace DB
{

using NATSSubscriptionPtr = std::unique_ptr<natsSubscription, decltype(&natsSubscription_Destroy)>;

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
    };

    bool isSubscribed() const;
    virtual void subscribe() = 0;
    void unsubscribe();

    /// Returns true if this consumer must be re-subscribed after a NATS
    /// reconnect (i.e. its subscription is NOT auto-restored by libnats).
    /// JetStream pull subscriptions need re-subscription because they are
    /// driven by application-level `PUB $JS.API.CONSUMER.MSG.NEXT` calls
    /// that the library does not auto-restore. Plain `Subscribe` and
    /// `QueueSubscribe` subscriptions are auto-restored by libnats and do
    /// not need re-subscription.
    virtual bool needsResubscribeOnReconnect() const = 0;

    size_t subjectsCount() { return subjects.size(); }

    bool isConsumerStopped() { return stopped; }

    bool queueEmpty() { return received.empty(); }
    size_t queueSize() { return received.size(); }

    auto getSubject() const { return current.subject; }
    const String & getCurrentMessage() const { return current.message; }

    /// Return read buffer containing next available message
    /// or nullptr if there are no messages to process.
    ReadBufferPtr consume();

protected:
    const NATSConnectionPtr & getConnection() { return connection; }
    natsConnection * getNativeConnection() { return connection->getConnection(); }

    const std::vector<String> & getSubjects() const { return subjects; }
    const LoggerPtr & getLogger() const { return log; }

    const String & getQueueName() const { return queue_name; }

    void setSubscriptions(std::vector<NATSSubscriptionPtr> subscriptions_) { subscriptions = std::move(subscriptions_); }

    static void onMsg(natsConnection * nc, natsSubscription * sub, natsMsg * msg, void * consumer);

private:
    NATSConnectionPtr connection;
    std::vector<NATSSubscriptionPtr> subscriptions;
    const std::vector<String> subjects;
    LoggerPtr log;
    const std::atomic<bool> & stopped;

    String queue_name;

    ConcurrentBoundedQueue<MessageData> received;
    MessageData current;
};

}
