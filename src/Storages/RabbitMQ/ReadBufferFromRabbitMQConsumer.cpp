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

static const auto QUEUE_SIZE = 50000;

ReadBufferFromRabbitMQConsumer::ReadBufferFromRabbitMQConsumer(
        ChannelPtr consumer_channel_,
        ChannelPtr setup_channel_,
        HandlerPtr event_handler_,
        const String & exchange_name_,
        size_t channel_id_,
        const String & queue_base_,
        Poco::Logger * log_,
        char row_delimiter_,
        bool hash_exchange_,
        size_t num_queues_,
        const String & deadletter_exchange_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(consumer_channel_))
        , setup_channel(setup_channel_)
        , event_handler(event_handler_)
        , exchange_name(exchange_name_)
        , channel_id(channel_id_)
        , queue_base(queue_base_)
        , hash_exchange(hash_exchange_)
        , num_queues(num_queues_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , stopped(stopped_)
        , deadletter_exchange(deadletter_exchange_)
        , received(QUEUE_SIZE * num_queues)
{
    for (size_t queue_id = 0; queue_id < num_queues; ++queue_id)
        bindQueue(queue_id);

    consumer_channel->onReady([&]()
    {
        consumer_channel->onError([&](const char * message)
        {
            LOG_ERROR(log, "Consumer {} error: {}", consumer_tag, message);
            channel_error.store(true);
        });

        subscribe();
    });
}


ReadBufferFromRabbitMQConsumer::~ReadBufferFromRabbitMQConsumer()
{
    consumer_channel->close();
    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromRabbitMQConsumer::bindQueue(size_t queue_id)
{
    std::atomic<bool> bindings_created = false, bindings_error = false;

    auto success_callback = [&](const std::string &  queue_name, int msgcount, int /* consumercount */)
    {
        queues.emplace_back(queue_name);
        LOG_DEBUG(log, "Queue {} is declared", queue_name);

        if (msgcount)
            LOG_TRACE(log, "Queue {} is non-empty. Non-consumed messaged will also be delivered", queue_name);

        /// Binding key must be a string integer in case of hash exchange (here it is either hash or fanout).
        setup_channel->bindQueue(exchange_name, queue_name, std::to_string(channel_id))
        .onSuccess([&]
        {
            bindings_created = true;
        })
        .onError([&](const char * message)
        {
            bindings_error = true;
            LOG_ERROR(log, "Failed to create queue binding. Reason: {}", message);
        });
    };

    auto error_callback([&](const char * message)
    {
        bindings_error = true;
        LOG_ERROR(log, "Failed to declare queue on the channel. Reason: {}", message);
    });

    AMQP::Table queue_settings;
    if (!deadletter_exchange.empty())
    {
        queue_settings["x-dead-letter-exchange"] = deadletter_exchange;
    }

    if (!queue_base.empty())
    {
        const String queue_name = !hash_exchange ? queue_base : queue_base + "_" + std::to_string(channel_id) + "_" + std::to_string(queue_id);
        setup_channel->declareQueue(queue_name, AMQP::durable, queue_settings).onSuccess(success_callback).onError(error_callback);
    }
    else
    {
        setup_channel->declareQueue(AMQP::durable, queue_settings).onSuccess(success_callback).onError(error_callback);
    }

    while (!bindings_created && !bindings_error)
    {
        iterateEventLoop();
    }
}


void ReadBufferFromRabbitMQConsumer::subscribe()
{
    for (const auto & queue_name : queues)
    {
        consumer_channel->consume(queue_name)
        .onSuccess([&](const std::string & consumer)
        {
            consumer_tag = consumer;
            LOG_TRACE(log, "Consumer {} (consumer tag: {}) is subscribed to queue {}", channel_id, consumer, queue_name);
        })
        .onReceived([&](const AMQP::Message & message, uint64_t delivery_tag, bool redelivered)
        {
            if (message.bodySize())
            {
                String message_received = std::string(message.body(), message.body() + message.bodySize());
                if (row_delimiter != '\0')
                    message_received += row_delimiter;

                received.push({delivery_tag, message_received, redelivered});
            }
        })
        .onError([&](const char * message)
        {
            LOG_ERROR(log, "Consumer {} failed. Reason: {}", channel_id, message);
        });
    }
}


void ReadBufferFromRabbitMQConsumer::ackMessages()
{
    UInt64 delivery_tag = last_inserted_delivery_tag;
    if (delivery_tag && delivery_tag > prev_tag)
    {
        prev_tag = delivery_tag;
        consumer_channel->ack(prev_tag, AMQP::multiple); /// Will ack all up to last tag staring from last acked.
        LOG_TRACE(log, "Consumer {} acknowledged messages with deliveryTags up to {}", consumer_tag, prev_tag);
    }
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


void ReadBufferFromRabbitMQConsumer::restoreChannel(ChannelPtr new_channel)
{
    if (consumer_channel->usable())
        return;

    consumer_channel = std::move(new_channel);
    consumer_channel->onReady([&]()
    {
        LOG_TRACE(log, "Channel {} is restored", channel_id);
        channel_error.store(false);
        consumer_channel->onError([&](const char * message)
        {
            LOG_ERROR(log, "Consumer {} error: {}", consumer_tag, message);
            channel_error.store(true);
        });

        subscribe();
    });
}


}
