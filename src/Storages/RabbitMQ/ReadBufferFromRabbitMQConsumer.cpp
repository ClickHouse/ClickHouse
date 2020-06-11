#include <utility>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <memory>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <common/logger_useful.h>
#include "Poco/Timer.h"
#include <amqpcpp.h>

namespace DB
{


namespace Exchange
{
    /// Note that default here means default by implementation and not by rabbitmq settings
    static const String DEFAULT = "default";
    static const String FANOUT = "fanout";
    static const String DIRECT = "direct";
    static const String TOPIC = "topic";
    static const String HASH = "consistent_hash";
}


ReadBufferFromRabbitMQConsumer::ReadBufferFromRabbitMQConsumer(
        ChannelPtr consumer_channel_,
        RabbitMQHandler & eventHandler_,
        const String & exchange_name_,
        const Names & routing_keys_,
        const size_t channel_id_,
        Poco::Logger * log_,
        char row_delimiter_,
        const bool bind_by_id_,
        const size_t num_queues_,
        const String & exchange_type_,
        const String table_name_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(consumer_channel_))
        , eventHandler(eventHandler_)
        , exchange_name(exchange_name_)
        , routing_keys(routing_keys_)
        , channel_id(channel_id_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , bind_by_id(bind_by_id_)
        , num_queues(num_queues_)
        , exchange_type(exchange_type_)
        , table_name(table_name_)
        , stopped(stopped_)
{
    messages.clear();
    current = messages.begin();

    exchange_type_set = exchange_type != Exchange::DEFAULT ? true : false;

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
    current = messages.begin();
    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromRabbitMQConsumer::initExchange()
{
    /* If exchange_type is not set - then direct-exchange is used - this type of exchange is the fastest (also due to different
     * binding algorithm this default behaviuor is much faster). It is also used in INSERT query.
     */
    String producer_exchange = exchange_type_set ? exchange_name + "_" + Exchange::DEFAULT : exchange_name;
    consumer_channel->declareExchange(producer_exchange, AMQP::fanout).onError([&](const char * message)
    {
        internal_exchange_declared = false;
        LOG_ERROR(log, "Failed to declare exchange: {}", message);
    });

    internal_exchange_name = producer_exchange + "_" + Exchange::DIRECT;
    consumer_channel->declareExchange(internal_exchange_name, AMQP::direct).onError([&](const char * message)
    {
        internal_exchange_declared = false;
        LOG_ERROR(log, "Failed to declare exchange: {}", message);
    });

    /// With fanout exchange the binding key is ignored - a parameter might be arbitrary
    consumer_channel->bindExchange(producer_exchange, internal_exchange_name, routing_keys[0]).onError([&](const char * message)
    {
        internal_exchange_declared = false;
        LOG_ERROR(log, "Failed to bind exchange: {}", message);
    });

    if (!exchange_type_set)
        return;

    /// For special purposes to use the flexibility of routing provided by rabbitmq - choosing exchange types is supported.

    AMQP::ExchangeType type;
    if      (exchange_type == Exchange::FANOUT)         type = AMQP::ExchangeType::fanout;
    else if (exchange_type == Exchange::DIRECT)         type = AMQP::ExchangeType::direct;
    else if (exchange_type == Exchange::TOPIC)          type = AMQP::ExchangeType::topic;
    else if (exchange_type == Exchange::HASH)           type = AMQP::ExchangeType::consistent_hash;
    else                                                return;

    /* Declare exchange of the specified type and bind it to hash-exchange, which will evenly distribute messages
     * between all consumers. (This enables better scaling as without hash-echange - the only oprion to avoid getting the same
     * messages more than once - is having only one consumer with one queue, which is not good.)
     */
    consumer_channel->declareExchange(exchange_name, type).onError([&](const char * message)
    {
        local_exchange_declared = false;
        LOG_ERROR(log, "Failed to declare {} exchange: {}", exchange_type, message);
    });

    /// No need for declaring hash-exchange if there is only one consumer with one queue and exchange type is not hash
    if (!bind_by_id && exchange_type != Exchange::HASH)
        return;

    hash_exchange = true;

    AMQP::Table exchange_arguments;
    exchange_arguments["hash-property"] = "message_id";

    local_exchange_name = exchange_name + "_" + table_name;
    consumer_channel->declareExchange(local_exchange_name, AMQP::consistent_hash, exchange_arguments)
    .onError([&](const char * message)
    {
        local_exchange_declared = false;
        LOG_ERROR(log, "Failed to declare {} exchange: {}", exchange_type, message);
    });

    for (auto & routing_key : routing_keys)
    {
        consumer_channel->bindExchange(exchange_name, local_exchange_name, routing_key).onError([&](const char * message)
        {
            local_exchange_declared = false;
            LOG_ERROR(log, "Failed to bind {} exchange to {} exchange: {}", local_exchange_name, exchange_name, message);
        });
    }
}


void ReadBufferFromRabbitMQConsumer::initQueueBindings(const size_t queue_id)
{
    /// These variables might be updated later from a separate thread in onError callbacks
    if (!internal_exchange_declared || (exchange_type_set && !local_exchange_declared))
    {
        initExchange();
        local_exchange_declared = true;
        internal_exchange_declared = true;
    }

    /* Internal exchange is a default exchange (by implementstion, not by rabbitmq settings) and is used for INSERT query
     * and if exchange_type is not set - there is no local exchange. If it is set - then local exchange is a distributor
     * exchange, which is bound to the exchange specified by the client.
     */
    bool internal_bindings_created = false, internal_bindings_error = false;
    bool local_bindings_created = false, local_bindings_error = false;

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

        consumer_channel->bindQueue(internal_exchange_name, queue_name_, binding_key)
        .onSuccess([&]
        {
            internal_bindings_created = true;
        })
        .onError([&](const char * message)
        {
            internal_bindings_error = true;
            LOG_ERROR(log, "Failed to bind to key {}. Reason: {}", binding_key, message);
        });

        /// Must be done here and not in readPrefix() because library might fail to handle async subscription on the same connection
        subscribe(queues.back());

        LOG_TRACE(log, "Queue " + queue_name_ + " is bound by key " + binding_key);

        if (exchange_type_set)
        {
            /// If hash-exchange is used for messages distribution, then the binding key is ignored - can be arbitrary
            if (hash_exchange)
            {
                consumer_channel->bindQueue(local_exchange_name, queue_name_, binding_key)
                .onSuccess([&]
                {
                    local_bindings_created = true;
                })
                .onError([&](const char * message)
                {
                    local_bindings_error = true;
                    LOG_ERROR(log, "Failed to create queue binding to key {}. Reason: {}", binding_key, message);
                });
            }
            else
            {
                /// Means there is only one queue with one consumer - no even distribution needed - no hash-exchange
                for (auto & routing_key : routing_keys)
                {
                    /// Binding directly to exchange, specified by the client
                    consumer_channel->bindQueue(exchange_name, queue_name_, routing_key)
                    .onSuccess([&]
                    {
                        local_bindings_created = true;
                    })
                    .onError([&](const char * message)
                    {
                        local_bindings_error = true;
                        LOG_ERROR(log, "Failed to create queue binding to key {}. Reason: {}", routing_key, message);
                    });
                }
            }
        }
    })
    .onError([&](const char * message)
    {
        internal_bindings_error = true;
        LOG_ERROR(log, "Failed to declare queue on the channel: {}", message);
    });

    /* Run event loop (which updates local variables in a separate thread) until bindings are created or failed to be created.
     * It is important at this moment to make sure that queue bindings are created before any publishing can happen because
     * otherwise messages will be routed nowhere.
     */
    while (!internal_bindings_created && !internal_bindings_error
            || (exchange_type_set && !local_bindings_created && !local_bindings_error))
    {
        startEventLoop(loop_started);
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

            bool stop_loop = false;

            /// Needed to avoid data race because this vector can be used at the same time by another thread in nextImpl().
            {
                std::lock_guard lock(mutex);
                received.push_back(message_received);

                /* As event loop is blocking to the thread that started it and a single thread should not be blocked while
                 * executing all callbacks on the connection (not only its own), then there should be some point to unblock.
                 * loop_started == 1 if current consumer is started the loop and not another.
                 */
                if (!loop_started)
                {
                    stop_loop = true;
                }
            }

            if (stop_loop)
            {
                stopEventLoopWithTimeout();
            }
        }
    })
    .onError([&](const char * message)
    {
        consumer_error = true;
        LOG_ERROR(log, "Consumer {} failed: {}", channel_id, message);
    });
}


void ReadBufferFromRabbitMQConsumer::checkSubscription()
{
    /// In general this condition will always be true and looping/resubscribing would not happen
    if (count_subscribed == num_queues)
        return;

    wait_subscribed = num_queues;

    /// These variables are updated in a separate thread
    while (count_subscribed != wait_subscribed && !consumer_error)
    {
        startEventLoop(loop_started);
    }

    LOG_TRACE(log, "Consumer {} is subscribed to {} queues", channel_id, count_subscribed);

    /// A case that would not normally happen
    for (auto & queue : queues)
    {
        subscribe(queue);
    }
}


void ReadBufferFromRabbitMQConsumer::stopEventLoop()
{
    eventHandler.stop();
}


void ReadBufferFromRabbitMQConsumer::stopEventLoopWithTimeout()
{
    eventHandler.stopWithTimeout();
}


void ReadBufferFromRabbitMQConsumer::startEventLoop(std::atomic<bool> & loop_started)
{
    eventHandler.startConsumerLoop(loop_started);
}


bool ReadBufferFromRabbitMQConsumer::nextImpl()
{
    if (stopped || !allowed)
        return false;

    if (current == messages.end())
    {
        if (received.empty())
        {
            /// Run the onReceived callbacks to save the messages that have been received by now, blocks current thread
            startEventLoop(loop_started);
            loop_started = false;
        }

        if (received.empty())
        {
            LOG_TRACE(log, "No more messages to be fetched");
            return false;
        }

        messages.clear();

        /// Needed to avoid data race because this vector can be used at the same time by another thread in onReceived callback.
        std::lock_guard lock(mutex);

        messages.swap(received);
        current = messages.begin();
    }

    auto * new_position = const_cast<char *>(current->data());
    BufferBase::set(new_position, current->size(), 0);

    ++current;
    allowed = false;

    return true;
}

}
