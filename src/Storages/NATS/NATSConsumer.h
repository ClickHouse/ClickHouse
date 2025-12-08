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

class NATSConsumer
{
public:
    NATSConsumer(
        NATSConnectionPtr connection_,
        std::vector<String> & subjects_,
        const String & subscribe_queue_name,
        LoggerPtr log_,
        uint32_t queue_size_,
        const std::atomic<bool> & stopped_);

    struct MessageData
    {
        String message;
        String subject;
    };

    bool isSubscribed() const;
    void subscribe();
    void unsubscribe();

    size_t subjectsCount() { return subjects.size(); }

    bool isConsumerStopped() { return stopped; }

    bool queueEmpty() { return received.empty(); }
    size_t queueSize() { return received.size(); }

    auto getSubject() const { return current.subject; }
    const String & getCurrentMessage() const { return current.message; }

    /// Return read buffer containing next available message
    /// or nullptr if there are no messages to process.
    ReadBufferPtr consume();

private:
    static void onMsg(natsConnection * nc, natsSubscription * sub, natsMsg * msg, void * consumer);

    NATSConnectionPtr connection;
    std::vector<NATSSubscriptionPtr> subscriptions;
    std::vector<String> subjects;
    LoggerPtr log;
    const std::atomic<bool> & stopped;

    String queue_name;

    String channel_id;
    ConcurrentBoundedQueue<MessageData> received;
    MessageData current;
};

}
