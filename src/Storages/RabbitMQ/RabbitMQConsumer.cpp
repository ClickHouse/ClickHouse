#include <utility>
#include <chrono>
#include <thread>
#include <atomic>
#include <memory>
#include <Storages/RabbitMQ/RabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/logger_useful.h>
#include "Poco/Timer.h"
#include <amqpcpp.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RabbitMQConsumer::RabbitMQConsumer(
        RabbitMQHandler & event_handler_,
        std::vector<String> & queues_,
        size_t channel_id_base_,
        const String & channel_base_,
        Poco::Logger * log_,
        uint32_t queue_size_)
        : event_handler(event_handler_)
        , queues(queues_)
        , channel_base(channel_base_)
        , channel_id_base(channel_id_base_)
        , log(log_)
        , received(queue_size_)
{
}

void RabbitMQConsumer::shutdown()
{
    stopped = true;
    cv.notify_one();
}

void RabbitMQConsumer::closeConnections()
{
    if (consumer_channel)
        consumer_channel->close();
}

void RabbitMQConsumer::subscribe()
{
    for (const auto & queue_name : queues)
    {
        consumer_channel->consume(queue_name)
        .onSuccess([&](const std::string & /* consumer_tag */)
        {
            LOG_TRACE(log, "Consumer on channel {} is subscribed to queue {}", channel_id, queue_name);

            if (++subscribed == queues.size())
                wait_subscription.store(false);
        })
        .onReceived([&](const AMQP::Message & message, uint64_t delivery_tag, bool redelivered)
        {
            if (message.bodySize())
            {
                String message_received = std::string(message.body(), message.body() + message.bodySize());

                if (!received.push({message_received, message.hasMessageID() ? message.messageID() : "",
                        message.hasTimestamp() ? message.timestamp() : 0,
                        redelivered, AckTracker(delivery_tag, channel_id)}))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to received queue");

                cv.notify_one();
            }
        })
        .onError([&](const char * message)
        {
            /* End up here either if channel ends up in an error state (then there will be resubscription) or consume call error, which
             * arises from queue settings mismatch or queue level error, which should not happen as no one else is supposed to touch them
             */
            LOG_ERROR(log, "Consumer failed on channel {}. Reason: {}", channel_id, message);
            wait_subscription.store(false);
        });
    }
}


bool RabbitMQConsumer::ackMessages()
{
    AckTracker record_info = last_inserted_record_info;

    /* Do not send ack to server if message's channel is not the same as current running channel because delivery tags are scoped per
     * channel, so if channel fails, all previous delivery tags become invalid
     */
    if (record_info.channel_id == channel_id && record_info.delivery_tag && record_info.delivery_tag > prev_tag)
    {
        /// Commit all received messages with delivery tags from last committed to last inserted
        if (!consumer_channel->ack(record_info.delivery_tag, AMQP::multiple))
        {
            LOG_ERROR(log, "Failed to commit messages with delivery tags from last committed to {} on channel {}",
                     record_info.delivery_tag, channel_id);
            return false;
        }

        prev_tag = record_info.delivery_tag;
        LOG_TRACE(log, "Consumer committed messages with deliveryTags up to {} on channel {}", record_info.delivery_tag, channel_id);
    }

    return true;
}


void RabbitMQConsumer::updateAckTracker(AckTracker record_info)
{
    if (record_info.delivery_tag && channel_error.load())
        return;

    if (!record_info.delivery_tag)
        prev_tag = 0;

    last_inserted_record_info = record_info;
}


void RabbitMQConsumer::setupChannel()
{
    if (!consumer_channel)
        return;

    wait_subscription.store(true);

    consumer_channel->onReady([&]()
    {
        /* First number indicates current consumer buffer; second number indicates serial number of created channel for current buffer,
         * i.e. if channel fails - another one is created and its serial number is incremented; channel_base is to guarantee that
         * channel_id is unique for each table
         */
        channel_id = std::to_string(channel_id_base) + "_" + std::to_string(channel_id_counter++) + "_" + channel_base;
        LOG_TRACE(log, "Channel {} is created", channel_id);

        subscribed = 0;
        subscribe();
        channel_error.store(false);
    });

    consumer_channel->onError([&](const char * message)
    {
        LOG_ERROR(log, "Channel {} error: {}", channel_id, message);

        channel_error.store(true);
        wait_subscription.store(false);
    });
}


bool RabbitMQConsumer::needChannelUpdate()
{
    if (wait_subscription)
        return false;

    return channel_error || !consumer_channel || !consumer_channel->usable();
}


void RabbitMQConsumer::iterateEventLoop()
{
    event_handler.iterateLoop();
}

ReadBufferPtr RabbitMQConsumer::consume()
{
    if (stopped || !received.tryPop(current))
        return nullptr;

    return std::make_shared<ReadBufferFromMemory>(current.message.data(), current.message.size());
}

}
