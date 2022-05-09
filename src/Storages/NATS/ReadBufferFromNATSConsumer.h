#pragma once

#include <Core/Names.h>
#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <amqpcpp.h>
#include <nats.h>
#include <Storages/NATS/NATSConnection.h>
#include <Common/ConcurrentBoundedQueue.h>

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
            NATSHandler & event_handler_,
            std::shared_ptr<NATSConnectionManager> connection_,
            std::vector<String> & subjects_,
            const String & channel_base_,
            Poco::Logger * log_,
            char row_delimiter_,
            uint32_t queue_size_,
            const std::atomic<bool> & stopped_);

    ~ReadBufferFromNATSConsumer() override;

//    struct AckTracker
//    {
//        UInt64 delivery_tag;
//        String channel_id;
//
//        AckTracker() = default;
//        AckTracker(UInt64 tag, String id) : delivery_tag(tag), channel_id(id) {}
//    };

    struct MessageData
    {
        String message;
        String subject;
        int64_t timestamp;
//        AckTracker track{};
    };

    std::vector<SubscriptionPtr> & getChannel() { return subscriptions; }
    void closeChannel()
    {
        for (const auto & subscription : subscriptions)
            natsSubscription_Unsubscribe(subscription.get());
    }

    void updateSubjects(std::vector<String> & subjects_) { subjects = subjects_; }
    size_t subjectsCount() { return subjects.size(); }

    bool isConsumerStopped() { return stopped; }
//    bool ackMessages();
//    void updateAckTracker(AckTracker record = AckTracker());

    bool queueEmpty() { return received.empty(); }
    void allowNext() { allowed = true; } // Allow to read next message.

    auto getSubject() const { return current.subject; }
    auto getTimestamp() const { return current.timestamp; }

    void iterateEventLoop();
private:
    bool nextImpl() override;

    void subscribe();
    static void onMsg(natsConnection *nc, natsSubscription *sub, natsMsg * msg, void * closure);

    NATSHandler & event_handler; /// Used concurrently, but is thread safe.
    std::shared_ptr<NATSConnectionManager> connection;
    std::vector<SubscriptionPtr> subscriptions;
    std::vector<String> subjects;
    const String channel_base;
//    const size_t channel_id_base;
    Poco::Logger * log;
    char row_delimiter;
    bool allowed = true;
    const std::atomic<bool> & stopped;

    String channel_id;
//    std::atomic<bool> channel_error = true, wait_subscription = false;
    ConcurrentBoundedQueue<MessageData> received;
    MessageData current;
//    size_t subscribed = 0;

//    AckTracker last_inserted_record_info;
//    UInt64 prev_tag = 0, channel_id_counter = 0;
};

}
