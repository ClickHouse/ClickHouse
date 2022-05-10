#include <utility>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <memory>
#include <Storages/NATS/ReadBufferFromNATSConsumer.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/bind.hpp>
#include <Common/logger_useful.h>
#include "Poco/Timer.h"
#include <amqpcpp.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReadBufferFromNATSConsumer::ReadBufferFromNATSConsumer(
        NATSHandler & event_handler_,
        std::shared_ptr<NATSConnectionManager> connection_,
        std::vector<String> & subjects_,
        const String & channel_base_,
        Poco::Logger * log_,
        char row_delimiter_,
        uint32_t queue_size_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , event_handler(event_handler_)
        , connection(connection_)
        , subjects(subjects_)
        , channel_base(channel_base_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , stopped(stopped_)
        , received(queue_size_)
{
    subscribe();
    LOG_DEBUG(log, "Started NATS consumer");
}


ReadBufferFromNATSConsumer::~ReadBufferFromNATSConsumer()
{
    for (const auto& subscription : subscriptions) {
        natsSubscription_Unsubscribe(subscription.get());
    }

    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromNATSConsumer::subscribe()
{
    for (const auto & subject : subjects) {
        SubscriptionPtr subscription = connection->createSubscription(subject, onMsg, this);
        if (subscription.get())
            subscriptions.emplace_back(std::move(subscription));
    }
//    for (const auto & queue_name : subjects)
//    {
//        consumer_channel->consume(queue_name)
//        .onSuccess([&](const std::string & /* consumer_tag */)
//        {
//            LOG_TRACE(log, "Consumer on channel {} is subscribed to queue {}", channel_id, queue_name);
//
//            if (++subscribed == subjects.size())
//                wait_subscription.store(false);
//        })
//        .onReceived([&](const AMQP::Message & message, uint64_t delivery_tag, bool redelivered)
//        {
//            if (message.bodySize())
//            {
//                String message_received = std::string(message.body(), message.body() + message.bodySize());
//                if (row_delimiter != '\0')
//                    message_received += row_delimiter;
//
//                if (!received.push({message_received, message.hasMessageID() ? message.messageID() : "",
//                        message.hasTimestamp() ? message.timestamp() : 0,
//                        redelivered, AckTracker(delivery_tag, channel_id)}))
//                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to received queue");
//            }
//        })
//        .onError([&](const char * message)
//        {
//            /* End up here either if channel ends up in an error state (then there will be resubscription) or consume call error, which
//             * arises from queue settings mismatch or queue level error, which should not happen as no one else is supposed to touch them
//             */
//            LOG_ERROR(log, "Consumer failed on channel {}. Reason: {}", channel_id, message);
//            wait_subscription.store(false);
//        });
//    }
}


//bool ReadBufferFromNATSConsumer::ackMessages()
//{
//    AckTracker record_info = last_inserted_record_info;
//
//    /* Do not send ack to server if message's channel is not the same as current running channel because delivery tags are scoped per
//     * channel, so if channel fails, all previous delivery tags become invalid
//     */
//    if (record_info.channel_id == channel_id && record_info.delivery_tag && record_info.delivery_tag > prev_tag)
//    {
//        /// Commit all received messages with delivery tags from last committed to last inserted
//        if (!consumer_channel->ack(record_info.delivery_tag, AMQP::multiple))
//        {
//            LOG_ERROR(log, "Failed to commit messages with delivery tags from last committed to {} on channel {}",
//                     record_info.delivery_tag, channel_id);
//            return false;
//        }
//
//        prev_tag = record_info.delivery_tag;
//        LOG_TRACE(log, "Consumer committed messages with deliveryTags up to {} on channel {}", record_info.delivery_tag, channel_id);
//    }
//
//    return true;
//}


//void ReadBufferFromNATSConsumer::updateAckTracker(AckTracker record_info)
//{
//    if (record_info.delivery_tag && channel_error.load())
//        return;
//
//    if (!record_info.delivery_tag)
//        prev_tag = 0;
//
//    last_inserted_record_info = record_info;
//}


void ReadBufferFromNATSConsumer::iterateEventLoop()
{
    event_handler.iterateLoop();
}


bool ReadBufferFromNATSConsumer::nextImpl()
{
    if (stopped || !allowed)
        return false;

    if (received.tryPop(current))
    {
        auto * new_position = const_cast<char *>(current.message.data());
        BufferBase::set(new_position, current.message.size(), 0);
        allowed = false;

        return true;
    }

    return false;
}

void ReadBufferFromNATSConsumer::onMsg(natsConnection *, natsSubscription *, natsMsg * msg, void * consumer)
{
    auto * buffer = static_cast<ReadBufferFromNATSConsumer *>(consumer);
    const int msg_length = natsMsg_GetDataLength(msg);
    LOG_DEBUG(buffer->log, "I'm getting something {} {}", msg_length, natsMsg_GetData(msg));

    if (msg_length)
    {
        String message_received = std::string(natsMsg_GetData(msg), msg_length);
        if (buffer->row_delimiter != '\0')
            message_received += buffer->row_delimiter;

        if (!buffer->received.push({
                .message = message_received,
                .subject = natsMsg_GetSubject(msg),
                .timestamp = natsMsg_GetTime(msg)
            }))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to received queue");
    }

    natsMsg_Destroy(msg);
}

}
