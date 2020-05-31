#include <utility>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <memory>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <common/logger_useful.h>
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
        , exchange_declared(false)
        , false_param(false)
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
    /* This varibale can be updated from a different thread in case of some error so its better to always check
     * whether exchange is in a working state and if not - declare it once again.
     */
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
        /// No need for timeouts as this event loop is blocking for the current thread and quits in case there are no active events
        startEventLoop(bindings_created);
    }
}


void ReadBufferFromRabbitMQConsumer::subscribeConsumer()
{
    if (subscribed)
        return;

    LOG_TRACE(log, "Subscribing to " + std::to_string(queues.size()) + " queues");

    for (auto & queue : queues)
    {
        subscribe(queue);
    }

    subscribed = true;
}


void ReadBufferFromRabbitMQConsumer::subscribe(const String & queue_name)
{
    std::atomic<bool> consumer_created = false, consumer_error = false;

    consumer_channel->consume(queue_name, AMQP::noack)
    .onSuccess([&](const std::string & /* consumer */)
    {
        consumer_created = true;

        LOG_TRACE(log, "Consumer " + std::to_string(channel_id) + " is subscribed to queue " + queue_name);
    })
    .onReceived([&](const AMQP::Message & message, uint64_t /* deliveryTag */, bool /* redelivered */)
    {
        size_t message_size = message.bodySize();
        if (message_size && message.body() != nullptr)
        {
            String message_received = std::string(message.body(), message.body() + message_size);

            if (row_delimiter != '\0')
                message_received += row_delimiter;

            /* Needed because this vector can be used at the same time by another thread in nextImpl() (below).
             * So we lock mutex here and there so that they do not use it asynchronosly.
             */
            std::lock_guard lock(mutex);
            received.push_back(message_received);
        }
    })
    .onError([&](const char * message)
    {
        consumer_error = true;
        LOG_ERROR(log, "Consumer failed: {}", message);
    });

    while (!consumer_created && !consumer_error)
    {
        /// No need for timeouts as this event loop is blocking for the current thread and quits in case there are no active events
        startEventLoop(consumer_created);
    }
}


void ReadBufferFromRabbitMQConsumer::startEventLoop(std::atomic<bool> & check_param)
{
    eventHandler.start(check_param);
}


bool ReadBufferFromRabbitMQConsumer::nextImpl()
{
    if (stopped || !allowed)
        return false;

    if (current == messages.end())
    {
        if (received.empty())
        {
            /// Run the onReceived callbacks to save the messages that have been received by now
            startEventLoop(false_param);
        }

        if (received.empty())
        {
            LOG_TRACE(log, "No more messages to be fetched");
            return false;
        }

        messages.clear();

        /* Needed because this vector can be used at the same time by another thread in onReceived callback (above).
         * So we lock mutex here and there so that they do not use it asynchronosly.
         */
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
