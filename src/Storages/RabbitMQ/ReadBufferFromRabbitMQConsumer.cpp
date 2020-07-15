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
}

namespace ExchangeType
{
    /// Note that default here means default by implementation and not by rabbitmq settings
    static const String DEFAULT = "default";
    static const String FANOUT = "fanout";
    static const String DIRECT = "direct";
    static const String TOPIC = "topic";
    static const String HASH = "consistent_hash";
    static const String HEADERS = "headers";
}

static const auto QUEUE_SIZE = 50000; /// Equals capacity of a single rabbitmq queue

ReadBufferFromRabbitMQConsumer::ReadBufferFromRabbitMQConsumer(
        ChannelPtr consumer_channel_,
        HandlerPtr event_handler_,
        const String & exchange_name_,
        const Names & routing_keys_,
        size_t channel_id_,
        Poco::Logger * log_,
        char row_delimiter_,
        bool bind_by_id_,
        size_t num_queues_,
        const String & exchange_type_,
        const String & local_exchange_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(consumer_channel_))
        , event_handler(event_handler_)
        , exchange_name(exchange_name_)
        , routing_keys(routing_keys_)
        , channel_id(channel_id_)
        , bind_by_id(bind_by_id_)
        , num_queues(num_queues_)
        , exchange_type(exchange_type_)
        , local_exchange(local_exchange_)
        , local_default_exchange(local_exchange + "_" + ExchangeType::DIRECT)
        , local_hash_exchange(local_exchange + "_" + ExchangeType::HASH)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , stopped(stopped_)
        , messages(QUEUE_SIZE * num_queues)
{
    exchange_type_set = exchange_type != ExchangeType::DEFAULT;

    /* One queue per consumer can handle up to 50000 messages. More queues per consumer can be added.
     * By default there is one queue per consumer.
     */
    for (size_t queue_id = 0; queue_id < num_queues; ++queue_id)
    {
        /// Queue bingings must be declared before any publishing => it must be done here and not in readPrefix()
        initQueueBindings(queue_id);
    }
}


ReadBufferFromRabbitMQConsumer::~ReadBufferFromRabbitMQConsumer()
{
    consumer_channel->close();

    messages.clear();
    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromRabbitMQConsumer::initExchange()
{
    /* This direct-exchange is used for default implemenation and for INSERT query (so it is always declared). If exchange_type
     * is not set, then there are only two exchanges - external, defined by the client, and local, unique for each table (default).
     * This strict division to external and local exchanges is needed to avoid too much complexity with defining exchange_name
     * for INSERT query producer and, in general, it is better to distinguish them into separate ones.
     */
    consumer_channel->declareExchange(local_default_exchange, AMQP::direct).onError([&](const char * message)
    {
        local_exchange_declared = false;
        LOG_ERROR(log, "Failed to declare local direct-exchange. Reason: {}", message);
    });

    if (!exchange_type_set)
    {
        consumer_channel->declareExchange(exchange_name, AMQP::fanout).onError([&](const char * message)
        {
            local_exchange_declared = false;
            LOG_ERROR(log, "Failed to declare default fanout-exchange. Reason: {}", message);
        });

        /// With fanout exchange the binding key is ignored - a parameter might be arbitrary. All distribution lies on local_exchange.
        consumer_channel->bindExchange(exchange_name, local_default_exchange, routing_keys[0]).onError([&](const char * message)
        {
            local_exchange_declared = false;
            LOG_ERROR(log, "Failed to bind local direct-exchange to fanout-exchange. Reason: {}", message);
        });

        return;
    }

    AMQP::ExchangeType type;
    if      (exchange_type == ExchangeType::FANOUT)         type = AMQP::ExchangeType::fanout;
    else if (exchange_type == ExchangeType::DIRECT)         type = AMQP::ExchangeType::direct;
    else if (exchange_type == ExchangeType::TOPIC)          type = AMQP::ExchangeType::topic;
    else if (exchange_type == ExchangeType::HASH)           type = AMQP::ExchangeType::consistent_hash;
    else if (exchange_type == ExchangeType::HEADERS)        type = AMQP::ExchangeType::headers;
    else throw Exception("Invalid exchange type", ErrorCodes::BAD_ARGUMENTS);

    /* Declare client's exchange of the specified type and bind it to hash-exchange (if it is not already hash-exchange), which
     * will evenly distribute messages between all consumers. (This enables better scaling as without hash-exchange - the only
     * option to avoid getting the same messages more than once - is having only one consumer with one queue)
     */
    consumer_channel->declareExchange(exchange_name, type).onError([&](const char * message)
    {
        local_exchange_declared = false;
        LOG_ERROR(log, "Failed to declare client's {} exchange. Reason: {}", exchange_type, message);
    });

    /// No need for declaring hash-exchange if there is only one consumer with one queue or exchange type is already hash
    if (!bind_by_id)
        return;

    hash_exchange = true;

    if (exchange_type == ExchangeType::HASH)
        return;

    /* By default hash exchange distributes messages based on a hash value of a routing key, which must be a string integer. But
     * in current case we use hash exchange for binding to another exchange of some other type, which needs its own routing keys
     * of other types: headers, patterns and string-keys. This means that hash property must be changed.
     */
    {
        AMQP::Table binding_arguments;
        binding_arguments["hash-property"] = "message_id";

        /// Declare exchange for sharding.
        consumer_channel->declareExchange(local_hash_exchange, AMQP::consistent_hash, binding_arguments)
        .onError([&](const char * message)
        {
            local_exchange_declared = false;
            LOG_ERROR(log, "Failed to declare {} exchange: {}", exchange_type, message);
        });
    }

    /// Then bind client's exchange to sharding exchange (by keys, specified by the client):

    if (exchange_type == ExchangeType::HEADERS)
    {
        AMQP::Table binding_arguments;
        std::vector<String> matching;

        for (const auto & header : routing_keys)
        {
            boost::split(matching, header, [](char c){ return c == '='; });
            binding_arguments[matching[0]] = matching[1];
            matching.clear();
        }

        /// Routing key can be arbitrary here.
        consumer_channel->bindExchange(exchange_name, local_hash_exchange, routing_keys[0], binding_arguments)
        .onError([&](const char * message)
        {
            local_exchange_declared = false;
            LOG_ERROR(log, "Failed to bind local hash exchange to client's exchange. Reason: {}", message);
        });
    }
    else
    {
        for (const auto & routing_key : routing_keys)
        {
            consumer_channel->bindExchange(exchange_name, local_hash_exchange, routing_key).onError([&](const char * message)
            {
                local_exchange_declared = false;
                LOG_ERROR(log, "Failed to bind local hash exchange to client's exchange. Reason: {}", message);
            });
        }
    }
}


void ReadBufferFromRabbitMQConsumer::initQueueBindings(const size_t queue_id)
{
    /// These variables might be updated later from a separate thread in onError callbacks.
    if (!local_exchange_declared || (exchange_type_set && !local_hash_exchange_declared))
    {
        initExchange();
        local_exchange_declared = true;
        local_hash_exchange_declared = true;
    }

    bool default_bindings_created = false, default_bindings_error = false;
    bool bindings_created = false, bindings_error = false;

    consumer_channel->declareQueue(AMQP::exclusive)
    .onSuccess([&](const std::string &  queue_name_, int /* msgcount */, int /* consumercount */)
    {
        queues.emplace_back(queue_name_);
        subscribed_queue[queue_name_] = false;

        String binding_key = routing_keys[0];

        /* Every consumer has at least one unique queue. Bind the queues to exchange based on the consumer_channel_id
         * in case there is one queue per consumer and bind by queue_id in case there is more than 1 queue per consumer.
         * (queue_id is based on channel_id)
         */
        if (bind_by_id || hash_exchange)
        {
            if (queues.size() == 1)
            {
                binding_key = std::to_string(channel_id);
            }
            else
            {
                binding_key = std::to_string(channel_id + queue_id);
            }
        }

        /// Bind queue to exchange that is used for INSERT query and also for default implementation.
        consumer_channel->bindQueue(local_default_exchange, queue_name_, binding_key)
        .onSuccess([&]
        {
            default_bindings_created = true;
        })
        .onError([&](const char * message)
        {
            default_bindings_error = true;
            LOG_ERROR(log, "Failed to bind to key {}. Reason: {}", binding_key, message);
        });

        /* Subscription can probably be moved back to readPrefix(), but not sure whether it is better in regard to speed, because
         * if moved there, it must(!) be wrapped inside a channel->onSuccess callback or any other, otherwise
         * consumer might fail to subscribe and no resubscription will help.
         */
        subscribe(queues.back());

        LOG_DEBUG(log, "Queue " + queue_name_ + " is declared");

        if (exchange_type_set)
        {
            if (hash_exchange)
            {
                /* If exchange_type == hash, then bind directly to this client's exchange (because there is no need for a distributor
                 * exchange as it is already hash-exchange), otherwise hash-exchange is a local distributor exchange.
                 */
                String current_hash_exchange = exchange_type == ExchangeType::HASH ? exchange_name : local_hash_exchange;

                /// If hash-exchange is used for messages distribution, then the binding key is ignored - can be arbitrary.
                consumer_channel->bindQueue(current_hash_exchange, queue_name_, binding_key)
                .onSuccess([&]
                {
                    bindings_created = true;
                })
                .onError([&](const char * message)
                {
                    bindings_error = true;
                    LOG_ERROR(log, "Failed to create queue binding to key {}. Reason: {}", binding_key, message);
                });
            }
            else if (exchange_type == ExchangeType::HEADERS)
            {
                AMQP::Table binding_arguments;
                std::vector<String> matching;

                /// It is not parsed for the second time - if it was parsed above, then it would never end up here.
                for (const auto & header : routing_keys)
                {
                    boost::split(matching, header, [](char c){ return c == '='; });
                    binding_arguments[matching[0]] = matching[1];
                    matching.clear();
                }

                consumer_channel->bindQueue(exchange_name, queue_name_, routing_keys[0], binding_arguments)
                .onSuccess([&]
                {
                    bindings_created = true;
                })
                .onError([&](const char * message)
                {
                    bindings_error = true;
                    LOG_ERROR(log, "Failed to bind queue to key. Reason: {}", message);
                });
            }
            else
            {
                /// Means there is only one queue with one consumer - no even distribution needed - no hash-exchange.
                for (const auto & routing_key : routing_keys)
                {
                    /// Binding directly to exchange, specified by the client.
                    consumer_channel->bindQueue(exchange_name, queue_name_, routing_key)
                    .onSuccess([&]
                    {
                        bindings_created = true;
                    })
                    .onError([&](const char * message)
                    {
                        bindings_error = true;
                        LOG_ERROR(log, "Failed to bind queue to key. Reason: {}", message);
                    });
                }
            }
        }
    })
    .onError([&](const char * message)
    {
        default_bindings_error = true;
        LOG_ERROR(log, "Failed to declare queue on the channel. Reason: {}", message);
    });

    /* Run event loop (which updates local variables in a separate thread) until bindings are created or failed to be created.
     * It is important at this moment to make sure that queue bindings are created before any publishing can happen because
     * otherwise messages will be routed nowhere.
     */
    while ((!default_bindings_created && !default_bindings_error) || (exchange_type_set && !bindings_created && !bindings_error))
    {
        iterateEventLoop();
    }
}


void ReadBufferFromRabbitMQConsumer::subscribe(const String & queue_name)
{
    if (subscribed_queue[queue_name])
        return;

    consumer_channel->consume(queue_name, AMQP::noack)
    .onSuccess([&](const std::string & /* consumer */)
    {
        subscribed_queue[queue_name] = true;
        consumer_error = false;
        ++count_subscribed;

        LOG_TRACE(log, "Consumer {} is subscribed to queue {}", channel_id, queue_name);
    })
    .onReceived([&](const AMQP::Message & message, uint64_t /* deliveryTag */, bool /* redelivered */)
    {
        size_t message_size = message.bodySize();
        if (message_size && message.body() != nullptr)
        {
            String message_received = std::string(message.body(), message.body() + message_size);
            if (row_delimiter != '\0')
            {
                message_received += row_delimiter;
            }

            messages.push(message_received);
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


void ReadBufferFromRabbitMQConsumer::iterateEventLoop()
{
    event_handler->iterateLoop();
}


bool ReadBufferFromRabbitMQConsumer::nextImpl()
{
    if (stopped || !allowed)
        return false;

    if (messages.tryPop(current))
    {
        auto * new_position = const_cast<char *>(current.data());
        BufferBase::set(new_position, current.size(), 0);
        allowed = false;

        return true;
    }

    return false;
}

}
