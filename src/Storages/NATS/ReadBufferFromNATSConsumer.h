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

class ReadBufferFromNATSConsumer : public ReadBuffer
{
public:
    ReadBufferFromNATSConsumer(
        std::shared_ptr<NATSConnectionManager> connection_,
        StorageNATS & storage_,
        std::vector<String> & subjects_,
        const String & subscribe_queue_name,
        Poco::Logger * log_,
        char row_delimiter_,
        uint32_t queue_size_,
        const std::atomic<bool> & stopped_);

    struct MessageData
    {
        String message;
        String subject;
    };

    void subscribe();
    void unsubscribe();

    size_t subjectsCount() { return subjects.size(); }

    bool isConsumerStopped() { return stopped; }

    bool queueEmpty() { return received.empty(); }
    size_t queueSize() { return received.size(); }
    void allowNext() { allowed = true; } // Allow to read next message.

    auto getSubject() const { return current.subject; }

private:
    bool nextImpl() override;

    static void onMsg(natsConnection * nc, natsSubscription * sub, natsMsg * msg, void * consumer);

    std::shared_ptr<NATSConnectionManager> connection;
    StorageNATS & storage;
    std::vector<SubscriptionPtr> subscriptions;
    std::vector<String> subjects;
    Poco::Logger * log;
    char row_delimiter;
    bool allowed = true;
    const std::atomic<bool> & stopped;

    bool subscribed = false;
    String queue_name;

    String channel_id;
    ConcurrentBoundedQueue<MessageData> received;
    MessageData current;
};

}
