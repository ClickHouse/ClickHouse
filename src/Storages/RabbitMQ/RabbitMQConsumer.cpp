#include <utility>
#include <chrono>
#include <thread>
#include <atomic>
#include <memory>
#include <Storages/RabbitMQ/RabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Storages/RabbitMQ/RabbitMQConnection.h>
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
        LoggerPtr log_,
        uint32_t queue_size_)
        : event_handler(event_handler_)
        , queues(queues_)
        , channel_base(channel_base_)
        , channel_id_base(channel_id_base_)
        , log(log_)
        , received(queue_size_)
{
}

void RabbitMQConsumer::stop()
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
            LOG_TRACE(
                log, "Consumer on channel {} ({}/{}) is subscribed to queue {}",
                channel_id, subscriptions_num, queues.size(), queue_name);
        })
        .onReceived([&](const AMQP::Message & message, uint64_t delivery_tag, bool redelivered)
        {
            if (message.bodySize())
            {
                String message_received = std::string(message.body(), message.body() + message.bodySize());

                MessageData result{
                    .message = message_received,
                    .message_id = message.hasMessageID() ? message.messageID() : "",
                    .timestamp = message.hasTimestamp() ? message.timestamp() : 0,
                    .redelivered = redelivered,
                    .delivery_tag = delivery_tag,
                    .channel_id = channel_id};

                if (!received.push(std::move(result)))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to received queue");

                cv.notify_one();
            }
        })
        .onError([&](const char * message)
        {
            /* End up here either if channel ends up in an error state (then there will be resubscription)
             * or consume call error, which arises from queue settings mismatch or queue level error,
             * which should not happen as no one else is supposed to touch them
             */
            LOG_ERROR(log, "Consumer failed on channel {}. Reason: {}", channel_id, message);
            state = State::ERROR;
        });
    }
}


bool RabbitMQConsumer::ackMessages(const CommitInfo & commit_info)
{
    if (state != State::OK)
    {
        LOG_TEST(log, "State is {}, will not ack messages", magic_enum::enum_name(state.load(std::memory_order_relaxed)));
        return false;
    }

    /// Do not send ack to server if message's channel is not the same as
    /// current running channel because delivery tags are scoped per channel,
    /// so if channel fails, all previous delivery tags become invalid.
    if (commit_info.channel_id != channel_id)
    {
        LOG_TEST(log, "Channel ID changed {} -> {}, will not ack messages", commit_info.channel_id, channel_id);
        return false;
    }

    for (const auto & delivery_tag : commit_info.failed_delivery_tags)
    {
        if (consumer_channel->reject(delivery_tag))
            LOG_TRACE(
                log, "Consumer rejected message with deliveryTag {} on channel {}",
                delivery_tag, channel_id);
        else
            LOG_WARNING(
                log, "Failed to reject message with deliveryTag {} on channel {}",
                delivery_tag, channel_id);
    }

    /// Duplicate ack?
    if (commit_info.delivery_tag > last_commited_delivery_tag
        && consumer_channel->ack(commit_info.delivery_tag, AMQP::multiple))
    {
        last_commited_delivery_tag = commit_info.delivery_tag;

        LOG_TRACE(
            log, "Consumer committed messages with deliveryTags up to {} on channel {}",
            last_commited_delivery_tag, channel_id);

        return true;
    }

    if (commit_info.delivery_tag)
    {
        LOG_ERROR(
            log,
            "Did not commit messages for {}:{}, (current commit point {}:{})",
            commit_info.channel_id, commit_info.delivery_tag,
            channel_id, last_commited_delivery_tag);
    }

    return false;
}

bool RabbitMQConsumer::nackMessages(const CommitInfo & commit_info)
{
    if (state != State::OK)
    {
        LOG_TEST(log, "State is {}, will not nack messages", magic_enum::enum_name(state.load(std::memory_order_relaxed)));
        return false;
    }

    /// Nothing to nack.
    if (!commit_info.delivery_tag || commit_info.delivery_tag <= last_commited_delivery_tag)
    {
        LOG_TEST(log, "Delivery tag is {}, last committed delivery tag: {}, Will not nack messages",
                 commit_info.delivery_tag, last_commited_delivery_tag);
        return false;
    }

    if (consumer_channel->reject(commit_info.delivery_tag, AMQP::multiple))
    {
        LOG_TRACE(
            log, "Consumer rejected messages with deliveryTags from {} to {} on channel {}",
            last_commited_delivery_tag, commit_info.delivery_tag, channel_id);

        return true;
    }

    LOG_ERROR(
        log,
        "Failed to reject messages for {}:{}, (current commit point {}:{})",
        commit_info.channel_id, commit_info.delivery_tag,
        channel_id, last_commited_delivery_tag);

    return false;
}

void RabbitMQConsumer::updateChannel(RabbitMQConnection & connection)
{
    state = State::INITIALIZING;
    last_commited_delivery_tag = 0;

    consumer_channel = connection.createChannel();
    consumer_channel->onReady([&]()
    {
        try
        {
            /// 1. channel_id_base - indicates current consumer buffer.
            /// 2. channel_id_couner - indicates serial number of created channel for current buffer
            ///    (incremented on each channel update).
            /// 3. channel_base is to guarantee that channel_id is unique for each table.
            channel_id = fmt::format("{}_{}_{}", channel_id_base, channel_id_counter++, channel_base);

            LOG_TRACE(log, "Channel {} is successfully created", channel_id);

            subscriptions_num = 0;
            subscribe();

            state = State::OK;
        }
        catch (...)
        {
            state = State::ERROR;
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    });

    consumer_channel->onError([&](const char * message)
    {
        LOG_ERROR(
            log, "Channel {} received an error: {} (usable: {}, connected: {})",
            channel_id, message, consumer_channel->usable(), consumer_channel->connected());

        if (!consumer_channel->usable() || !consumer_channel->connected())
        {
            state = State::ERROR;
        }
    });
}


bool RabbitMQConsumer::needChannelUpdate()
{
    chassert(consumer_channel);
    return state == State::ERROR;
}


ReadBufferPtr RabbitMQConsumer::consume()
{
    if (stopped || !received.tryPop(current))
        return nullptr;

    return std::make_shared<ReadBufferFromMemory>(current.message.data(), current.message.size());
}

}
