#include <utility>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <common/logger_useful.h>
#include <amqpcpp.h>


namespace DB
{

ReadBufferFromRabbitMQConsumer::ReadBufferFromRabbitMQConsumer(
        ChannelPtr channel_,
        RabbitMQHandler & eventHandler_,
        const String & exchange_name_,
        const String & routing_key_,
        Poco::Logger * log_,
        char row_delimiter_,
        const bool hash_exchange_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(channel_))
        , eventHandler(eventHandler_)
        , exchange_name(exchange_name_)
        , routing_key(routing_key_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , hash_exchange(hash_exchange_)
        , stopped(stopped_)
{
    messages.clear();
    current = messages.begin();

    initQueueBindings();
}


ReadBufferFromRabbitMQConsumer::~ReadBufferFromRabbitMQConsumer()
{
    //unsubscribe();
    consumer_channel->close();

    messages.clear();
    current = messages.begin();
    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromRabbitMQConsumer::initExchange()
{

    if (hash_exchange)
    {
        /* In the current implementation all the messages are send to exchange of direct type, which distributes messages to all
         * queues, which are bound to it with the routing key of the message. If hash_exchange flag is set,
         * it means that there should be a distribution of messages between multiple consumers - like rebalance.
         * In this case we bind the direct exchange to a consistent-hash exchange, which will distribute messages between all queues
         * based on a hash value (queues are unique to consumer channels so every message is delivered once). 
         * */
        consumer_channel->declareExchange(exchange_name, AMQP::fanout).onError([&](const char * message)
        {
            exchange_declared = false;
            LOG_ERROR(log, "Failed to declare exchange: " << message);
        });

        hash_exchange_name = exchange_name + "_hash";
        consumer_channel->declareExchange(hash_exchange_name, AMQP::consistent_hash).onError([&](const char * message)
        {
            exchange_declared = false;
        });

        consumer_channel->bindExchange(exchange_name, hash_exchange_name, routing_key).onError([&](const char * message)
        {
            exchange_declared = false;
        });
    }
    else
    {
        consumer_channel->declareExchange(exchange_name, AMQP::direct).onError([&](const char * message)
        {
            exchange_declared = false;
            LOG_ERROR(log, "Failed to declare exchange: " << message);
        });
    }
}


void ReadBufferFromRabbitMQConsumer::initQueueBindings()
{
    if (!exchange_declared)
    {
        initExchange();
        exchange_declared = true;
    }

    bool bindings_ok = false, bindings_error = false;

    consumer_channel->declareQueue(AMQP::exclusive)
        .onSuccess([&](const std::string &  queue_name_, int /* msgcount */, int /* consumercount */)
        {
            queue_name = queue_name_;
            String exchange_to_bind = exchange_name;

            if (hash_exchange)
            {
                exchange_to_bind = hash_exchange_name;
            }

            consumer_channel->bindQueue(exchange_to_bind, "", routing_key)
            .onSuccess([&]
            {
                bindings_ok = true;
            })
            .onError([&](const char * message)
            {
                bindings_error = true;
                LOG_ERROR(log, "Failed to create queue binding: " << message);
            });
    })
    .onError([&](const char * message)
    {
        bindings_error = true;
        LOG_ERROR(log, "Failed to declare queue on the channel: " << message);
    });

    while (!bindings_ok && !bindings_error)
    {
        startNonBlockEventLoop();
    }
}


void ReadBufferFromRabbitMQConsumer::subscribe()
{
    if (consumer_ok)
        return;

    consumer_channel->consume(queue_name, AMQP::noack)
    .onSuccess([&](const std::string & consumer)
    {
        if (consumerTag == "")
            consumerTag = consumer;

        consumer_ok = true;

        LOG_TRACE(log, "Consumer " + consumerTag + " is subscribed to queue " + queue_name);
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

            received.emplace_back(message_received);
            this->stalled = false; 
        }
    })
    .onError([&](const char * message)
    {
        consumer_error = true;
        LOG_ERROR(log, "Failed to create consumer: " << message);
    });

    while (!consumer_ok && !consumer_error)
    {
        startNonBlockEventLoop();
    }
}


void ReadBufferFromRabbitMQConsumer::unsubscribe()
{
    if (consumer_channel->usable() && consumerTag != "")
    {
        consumer_channel->cancel(consumerTag);
    }
}


void ReadBufferFromRabbitMQConsumer::startNonBlockEventLoop()
{
    eventHandler.startNonBlock();
}


void ReadBufferFromRabbitMQConsumer::stopEventLoop()
{
    eventHandler.stop();
}


bool ReadBufferFromRabbitMQConsumer::nextImpl()
{
    if (stalled || stopped)
        return false;

    if (current == messages.end())
    {
        startNonBlockEventLoop();

        if (received.empty())
        {
            LOG_TRACE(log, "Stalled");
            stalled = true;

            return false;
        }

        messages = received;
        received.clear();
        current = messages.begin();
    }

    auto new_position = const_cast<char *>(current->data());
    BufferBase::set(new_position, current->size(), 0);

    ++current;

    return true;
}

}
