#include <utility>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <memory>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <boost/algorithm/string/split.hpp>
#include <common/logger_useful.h>
#include "Poco/Timer.h"
#include <amqpcpp.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CREATE_RABBITMQ_QUEUE_BINDING;
}

ReadBufferFromRabbitMQConsumer::ReadBufferFromRabbitMQConsumer(
        ChannelPtr consumer_channel_,
        ChannelPtr setup_channel_,
        HandlerPtr event_handler_,
        const String & exchange_name_,
        size_t channel_id_base_,
        const String & channel_base_,
        const String & queue_base_,
        Poco::Logger * log_,
        char row_delimiter_,
        bool hash_exchange_,
        size_t num_queues_,
        const String & deadletter_exchange_,
        uint32_t queue_size_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(consumer_channel_))
        , setup_channel(setup_channel_)
        , event_handler(event_handler_)
        , exchange_name(exchange_name_)
        , channel_base(channel_base_)
        , channel_id_base(channel_id_base_)
        , queue_base(queue_base_)
        , hash_exchange(hash_exchange_)
        , num_queues(num_queues_)
        , deadletter_exchange(deadletter_exchange_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , queue_size(queue_size_)
        , stopped(stopped_)
        , received(queue_size * num_queues)
{
    for (size_t queue_id = 0; queue_id < num_queues; ++queue_id)
        bindQueue(queue_id);

    setupChannel();
}


ReadBufferFromRabbitMQConsumer::~ReadBufferFromRabbitMQConsumer()
{
    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromRabbitMQConsumer::bindQueue(size_t queue_id)
{
    std::atomic<bool> binding_created = false;

    auto success_callback = [&](const std::string &  queue_name, int msgcount, int /* consumercount */)
    {
        queues.emplace_back(queue_name);
        LOG_DEBUG(log, "Queue {} is declared", queue_name);

        if (msgcount)
            LOG_INFO(log, "Queue {} is non-empty. Non-consumed messaged will also be delivered", queue_name);

       /* Here we bind either to sharding exchange (consistent-hash) or to bridge exchange (fanout). All bindings to routing keys are
        * done between client's exchange and local bridge exchange. Binding key must be a string integer in case of hash exchange, for
        * fanout exchange it can be arbitrary
        */
        setup_channel->bindQueue(exchange_name, queue_name, std::to_string(channel_id_base))
        .onSuccess([&] { binding_created = true; })
        .onError([&](const char * message)
        {
            throw Exception(
                ErrorCodes::CANNOT_CREATE_RABBITMQ_QUEUE_BINDING,
                "Failed to create queue binding with queue {} for exchange {}. Reason: {}", std::string(message),
                queue_name, exchange_name);
        });
    };

    auto error_callback([&](const char * message)
    {
        /* This error is most likely a result of an attempt to declare queue with different settings if it was declared before. So for a
         * given queue name either deadletter_exchange parameter changed or queue_size changed, i.e. table was declared with different
         * max_block_size parameter. Solution: client should specify a different queue_base parameter or manually delete previously
         * declared queues via any of the various cli tools.
         */
        throw Exception("Failed to declare queue. Probably queue settings are conflicting: max_block_size, deadletter_exchange. Attempt \
                specifying differently those settings or use a different queue_base or manually delete previously declared queues,      \
                which  were declared with the same names. ERROR reason: "
                + std::string(message), ErrorCodes::BAD_ARGUMENTS);
    });

    AMQP::Table queue_settings;

    queue_settings["x-max-length"] = queue_size;
    queue_settings["x-overflow"] = "reject-publish";

    if (!deadletter_exchange.empty())
        queue_settings["x-dead-letter-exchange"] = deadletter_exchange;

    /* The first option not just simplifies queue_name, but also implements the possibility to be able to resume reading from one
     * specific queue when its name is specified in queue_base setting
     */
    const String queue_name = !hash_exchange ? queue_base : std::to_string(channel_id_base) + "_" + std::to_string(queue_id) + "_" + queue_base;
    setup_channel->declareQueue(queue_name, AMQP::durable, queue_settings).onSuccess(success_callback).onError(error_callback);

    while (!binding_created)
    {
        iterateEventLoop();
    }
}


void ReadBufferFromRabbitMQConsumer::subscribe()
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
                if (row_delimiter != '\0')
                    message_received += row_delimiter;

                if (message.hasMessageID())
                    received.push({message_received, message.messageID(), redelivered, AckTracker(delivery_tag, channel_id)});
                else
                    received.push({message_received, "", redelivered, AckTracker(delivery_tag, channel_id)});
            }
        })
        .onError([&](const char * message)
        {
            /* End up here either if channel ends up in an error state (then there will be resubscription) or consume call error, which
             * arises from queue settings mismatch or queue level error, which should not happen as noone else is supposed to touch them
             */
            LOG_ERROR(log, "Consumer failed on channel {}. Reason: {}", channel_id, message);
            wait_subscription.store(false);
        });
    }
}


bool ReadBufferFromRabbitMQConsumer::ackMessages()
{
    AckTracker record_info = last_inserted_record_info;

    /* Do not send ack to server if message's channel is not the same as current running channel because delivery tags are scoped per
     * channel, so if channel fails, all previous delivery tags become invalid
     */
    if (record_info.channel_id == channel_id && record_info.delivery_tag && record_info.delivery_tag > prev_tag)
    {
        /// Commit all received messages with delivery tags from last commited to last inserted
        if (!consumer_channel->ack(record_info.delivery_tag, AMQP::multiple))
        {
            LOG_ERROR(log, "Failed to commit messages with delivery tags from last commited to {} on channel {}",
                     record_info.delivery_tag, channel_id);
            return false;
        }

        prev_tag = record_info.delivery_tag;
        LOG_TRACE(log, "Consumer commited messages with deliveryTags up to {} on channel {}", record_info.delivery_tag, channel_id);
    }

    return true;
}


void ReadBufferFromRabbitMQConsumer::updateAckTracker(AckTracker record_info)
{
    if (record_info.delivery_tag && channel_error.load())
        return;

    if (!record_info.delivery_tag)
        prev_tag = 0;

    last_inserted_record_info = record_info;
}


void ReadBufferFromRabbitMQConsumer::setupChannel()
{
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


void ReadBufferFromRabbitMQConsumer::iterateEventLoop()
{
    event_handler->iterateLoop();
}


bool ReadBufferFromRabbitMQConsumer::nextImpl()
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

}
