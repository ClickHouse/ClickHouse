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

static const auto QUEUE_SIZE = 50000; /// Equals capacity of a single rabbitmq queue

ReadBufferFromRabbitMQConsumer::ReadBufferFromRabbitMQConsumer(
        ChannelPtr consumer_channel_,
        ChannelPtr setup_channel_,
        HandlerPtr event_handler_,
        const String & exchange_name_,
        const AMQP::ExchangeType & exchange_type_,
        const Names & routing_keys_,
        size_t channel_id_,
        const String & queue_base_,
        Poco::Logger * log_,
        char row_delimiter_,
        bool hash_exchange_,
        size_t num_queues_,
        const String & local_exchange_,
        const String & deadletter_exchange_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(consumer_channel_))
        , setup_channel(setup_channel_)
        , event_handler(event_handler_)
        , exchange_name(exchange_name_)
        , exchange_type(exchange_type_)
        , routing_keys(routing_keys_)
        , channel_id(channel_id_)
        , queue_base(queue_base_)
        , hash_exchange(hash_exchange_)
        , num_queues(num_queues_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , stopped(stopped_)
        , local_exchange(local_exchange_)
        , deadletter_exchange(deadletter_exchange_)
        , received(QUEUE_SIZE * num_queues)
{
    for (size_t queue_id = 0; queue_id < num_queues; ++queue_id)
        initQueueBindings(queue_id);
}


ReadBufferFromRabbitMQConsumer::~ReadBufferFromRabbitMQConsumer()
{
    if (ack.load() && consumer_channel)
    {
        consumer_channel->ack(prev_tag, AMQP::multiple); /// Will ack all up to last tag staring from last acked.
        LOG_TRACE(log, "Acknowledged messages with deliveryTags up to {}", prev_tag);
    }

    consumer_channel->close();
    received.clear();
    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromRabbitMQConsumer::initQueueBindings(const size_t queue_id)
{
    bool bindings_created = false, bindings_error = false;

    auto success_callback = [&](const std::string &  queue_name_, int msgcount, int /* consumercount */)
    {
        queues.emplace_back(queue_name_);
        LOG_DEBUG(log, "Queue " + queue_name_ + " is declared");

        if (msgcount)
            LOG_TRACE(log, "Queue " + queue_name_ + " is non-empty. Non-consumed messaged will also be delivered.");

        subscribed_queue[queue_name_] = false;
        subscribe(queues.back());

        if (hash_exchange)
        {
            String binding_key;
            if (queues.size() == 1)
                binding_key = std::to_string(channel_id);
            else
                binding_key = std::to_string(channel_id + queue_id);

            /* If exchange_type == hash, then bind directly to this client's exchange (because there is no need for a distributor
             * exchange as it is already hash-exchange), otherwise hash-exchange is a local distributor exchange.
             */
            String current_hash_exchange = exchange_type == AMQP::ExchangeType::consistent_hash ? exchange_name : local_exchange;

            setup_channel->bindQueue(current_hash_exchange, queue_name_, binding_key)
            .onSuccess([&]
            {
                bindings_created = true;
            })
            .onError([&](const char * message)
            {
                bindings_error = true;
                LOG_ERROR(log, "Failed to create queue binding. Reason: {}", message);
            });
        }
        else if (exchange_type == AMQP::ExchangeType::fanout)
        {
            setup_channel->bindQueue(exchange_name, queue_name_, routing_keys[0])
            .onSuccess([&]
            {
                bindings_created = true;
            })
            .onError([&](const char * message)
            {
                bindings_error = true;
                LOG_ERROR(log, "Failed to bind to key. Reason: {}", message);
            });
        }
        else if (exchange_type == AMQP::ExchangeType::headers)
        {
            AMQP::Table binding_arguments;
            std::vector<String> matching;

            for (const auto & header : routing_keys)
            {
                boost::split(matching, header, [](char c){ return c == '='; });
                binding_arguments[matching[0]] = matching[1];
                matching.clear();
            }

            setup_channel->bindQueue(exchange_name, queue_name_, routing_keys[0], binding_arguments)
            .onSuccess([&]
            {
                bindings_created = true;
            })
            .onError([&](const char * message)
            {
                bindings_error = true;
                LOG_ERROR(log, "Failed to bind queue. Reason: {}", message);
            });
        }
        else
        {
            /// Means there is only one queue with one consumer - no even distribution needed - no hash-exchange.
            for (const auto & routing_key : routing_keys)
            {
                /// Binding directly to exchange, specified by the client.
                setup_channel->bindQueue(exchange_name, queue_name_, routing_key)
                .onSuccess([&]
                {
                    bindings_created = true;
                })
                .onError([&](const char * message)
                {
                    bindings_error = true;
                    LOG_ERROR(log, "Failed to bind queue. Reason: {}", message);
                });
            }
        }
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


void ReadBufferFromRabbitMQConsumer::subscribe(const String & queue_name)
{
    if (subscribed_queue[queue_name])
        return;

    consumer_channel->consume(queue_name)
    .onSuccess([&](const std::string & consumer)
    {
        subscribed_queue[queue_name] = true;
        ++count_subscribed;
        LOG_TRACE(log, "Consumer {} is subscribed to queue {}", channel_id, queue_name);

        consumer_error = false;
        consumer_tag = consumer;

        consumer_channel->onError([&](const char * message)
        {
            LOG_ERROR(log, "Consumer {} error: {}", consumer_tag, message);
        });
    })
    .onReceived([&](const AMQP::Message & message, uint64_t deliveryTag, bool redelivered)
    {
        size_t message_size = message.bodySize();
        if (message_size && message.body() != nullptr)
        {
            String message_received = std::string(message.body(), message.body() + message_size);
            if (row_delimiter != '\0')
                message_received += row_delimiter;

            received.push({deliveryTag, message_received, redelivered});

            std::lock_guard lock(wait_ack);
            if (ack.exchange(false) && prev_tag < max_tag && consumer_channel)
            {
                consumer_channel->ack(prev_tag, AMQP::multiple); /// Will ack all up to last tag staring from last acked.
                LOG_TRACE(log, "Consumer {} acknowledged messages with deliveryTags up to {}", consumer_tag, prev_tag);
            }
        }
    })
    .onError([&](const char * message)
    {
        consumer_error = true;
        LOG_ERROR(log, "Consumer {} failed. Reason: {}", channel_id, message);
    });
}


void ReadBufferFromRabbitMQConsumer::checkSubscription()
{
    if (count_subscribed == num_queues)
        return;

    wait_subscribed = num_queues;

    /// These variables are updated in a separate thread.
    while (count_subscribed != wait_subscribed && !consumer_error)
    {
        iterateEventLoop();
    }

    LOG_TRACE(log, "Consumer {} is subscribed to {} queues", channel_id, count_subscribed);

    /// Updated in callbacks which are run by the loop.
    if (count_subscribed == num_queues)
        return;

    /// A case that should never normally happen.
    for (auto & queue : queues)
    {
        subscribe(queue);
    }
}


void ReadBufferFromRabbitMQConsumer::ackMessages(UInt64 last_inserted_delivery_tag)
{
    if (last_inserted_delivery_tag > prev_tag)
    {
        std::lock_guard lock(wait_ack);
        prev_tag = last_inserted_delivery_tag;
        ack.store(true);
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
        max_tag = current.delivery_tag;

        return true;
    }

    return false;
}

}
