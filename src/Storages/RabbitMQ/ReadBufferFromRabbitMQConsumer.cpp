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


ReadBufferFromRabbitMQConsumer::ReadBufferFromRabbitMQConsumer(
        ChannelPtr consumer_channel_,
        RabbitMQHandler & eventHandler_,
        const String & exchange_name_,
        const String & routing_key_,
        const size_t channel_id_,
        Poco::Logger * log_,
        char row_delimiter_,
        const bool bind_by_id_,
        const bool hash_exchange_,
        const size_t num_queues_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(consumer_channel_))
        , eventHandler(eventHandler_)
        , exchange_name(exchange_name_)
        , routing_key(routing_key_)
        , channel_id(channel_id_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , bind_by_id(bind_by_id_)
        , hash_exchange(hash_exchange_)
        , num_queues(num_queues_)
        , stopped(stopped_)
{
    messages.clear();
    current = messages.begin();

    /* One queue per consumer can handle up to 50000 messages. More queues per consumer can be added.
     * By default there is one queue per consumer.
     */
    for (size_t queue_id = 0; queue_id < num_queues; ++queue_id)
    {
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
    /* As there are 5 different types of exchanges and the type should be set as a parameter while publishing the message,
     * then for uniformity this parameter should always be set as fanout-exchange type. In current implementation, the exchange,
     * to which messages a published, will be bound to the exchange of the needed type, which will distribute messages according to its type.
     */
    consumer_channel->declareExchange(exchange_name, AMQP::fanout).onError([&](const char * message)
    {
        exchange_declared = false;
        LOG_ERROR(log, "Failed to declare fanout exchange: {}", message);
    });

    if (hash_exchange)
    {
        current_exchange_name = exchange_name + "_hash";
        consumer_channel->declareExchange(current_exchange_name, AMQP::consistent_hash).onError([&](const char * /* message */)
        {
            exchange_declared = false;
        });

        consumer_channel->bindExchange(exchange_name, current_exchange_name, routing_key).onError([&](const char * /* message */)
        {
            exchange_declared = false;
        });
    }
    else
    {
        current_exchange_name = exchange_name + "_direct";
        consumer_channel->declareExchange(current_exchange_name, AMQP::direct).onError([&](const char * /* message */)
        {
            exchange_declared = false;
        });

        consumer_channel->bindExchange(exchange_name, current_exchange_name, routing_key).onError([&](const char * /* message */)
        {
            exchange_declared = false;
        });
    }
}


void ReadBufferFromRabbitMQConsumer::initQueueBindings(const size_t queue_id)
{
    if (!exchange_declared)
    {
        initExchange();
        exchange_declared = true;
    }

    std::atomic<bool> bindings_created = false, bindings_error = false;

    consumer_channel->declareQueue(AMQP::exclusive)
    .onSuccess([&](const std::string &  queue_name_, int /* msgcount */, int /* consumercount */)
    {
        queues.emplace_back(queue_name_);
        subscribed_queue[queue_name_] = false;

        String binding_key = routing_key;

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

        /// Must be done here, cannot be done in readPrefix()
        subscribe(queues.back());

        LOG_TRACE(log, "Queue " + queue_name_ + " is bound by key " + binding_key);

        consumer_channel->bindQueue(current_exchange_name, queue_name_, binding_key)
        .onSuccess([&]
        {
            bindings_created = true;
        })
        .onError([&](const char * message)
        {
            bindings_error = true;
            LOG_ERROR(log, "Failed to create queue binding: {}", message);
        });
    })
    .onError([&](const char * message)
    {
        bindings_error = true;
        LOG_ERROR(log, "Failed to declare queue on the channel: {}", message);
    });

    /* Run event loop (which updates local variables in a separate thread) until bindings are created or failed to be created.
     * It is important at this moment to make sure that queue bindings are created before any publishing can happen because
     * otherwise messages will be routed nowhere.
     */
    while (!bindings_created && !bindings_error)
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
