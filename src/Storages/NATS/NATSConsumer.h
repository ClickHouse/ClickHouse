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

class INATSConsumer
{
public:
    struct MessageData
    {
        String message;
        String subject;
    };

    virtual void subscribe() = 0;
    virtual void unsubscribe() = 0;

    virtual size_t subjectsCount() = 0;

    virtual bool isConsumerStopped() = 0;

    virtual bool queueEmpty() = 0;
    virtual size_t queueSize() = 0;

    virtual String getSubject() const = 0;

    /// Return read buffer containing next available message
    /// or nullptr if there are no messages to process.
    virtual ReadBufferPtr consume() = 0;

    virtual ~INATSConsumer() = default;
};


class NATSJetStreamConsumer : public INATSConsumer
{
public:
    NATSJetStreamConsumer(
        std::shared_ptr<NATSConnectionManager> connection_,
        StorageNATS & storage_,
        std::vector<String> & subjects_,
        String & stream_,
        String & consumer_,
        Poco::Logger * log_,
        uint32_t queue_size_,
        const std::atomic<bool> & stopped_);

    void subscribe() override;
    void unsubscribe() override;

    size_t subjectsCount() override { return subjects.size(); }

    bool isConsumerStopped() override { return stopped; }

    bool queueEmpty() override { return received.empty(); }
    size_t queueSize() override { return received.size(); }

    String getSubject() const override { return current.subject; }

    /// Return read buffer containing next available message
    /// or nullptr if there are no messages to process.
    ReadBufferPtr consume() override;

private:
    static void onMsg(natsConnection * nc, natsSubscription * sub, natsMsg * msg, void * consumer);

    std::shared_ptr<NATSConnectionManager> connection;
    StorageNATS & storage;
    std::vector<SubscriptionPtr> subscriptions;
    std::vector<String> subjects;

    String stream;
    String consumer;

    Poco::Logger * log;
    const std::atomic<bool> & stopped;

    bool subscribed = false;

    String channel_id;
    ConcurrentBoundedQueue<MessageData> received;
    MessageData current;

    std::unique_ptr<jsCtx, decltype(&jsCtx_Destroy)> js;
};


class NATSConsumer : public INATSConsumer
{
public:
    NATSConsumer(
        std::shared_ptr<NATSConnectionManager> connection_,
        StorageNATS & storage_,
        std::vector<String> & subjects_,
        const String & subscribe_queue_name,
        Poco::Logger * log_,
        uint32_t queue_size_,
        const std::atomic<bool> & stopped_);

    void subscribe() override;
    void unsubscribe() override;

    size_t subjectsCount() override { return subjects.size(); }

    bool isConsumerStopped() override { return stopped; }

    bool queueEmpty() override { return received.empty(); }
    size_t queueSize() override { return received.size(); }

    String getSubject() const override { return current.subject; }

    /// Return read buffer containing next available message
    /// or nullptr if there are no messages to process.
    ReadBufferPtr consume() override;

private:
    static void onMsg(natsConnection * nc, natsSubscription * sub, natsMsg * msg, void * consumer);

    std::shared_ptr<NATSConnectionManager> connection;
    StorageNATS & storage;
    std::vector<SubscriptionPtr> subscriptions;
    std::vector<String> subjects;
    Poco::Logger * log;
    const std::atomic<bool> & stopped;

    bool subscribed = false;
    String queue_name;

    String channel_id;
    ConcurrentBoundedQueue<MessageData> received;
    MessageData current;
};

}
