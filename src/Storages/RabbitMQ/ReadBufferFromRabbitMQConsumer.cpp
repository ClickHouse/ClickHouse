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
        Poco::Logger * log_,
        char row_delimiter_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(channel_))
        , eventHandler(eventHandler_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , stopped(stopped_)
{
    consumer_channel->confirmSelect()
        .onError([&](const char * message)
        {
            LOG_ERROR(log, "Error with the consumer channel - " << message);
        });

    messages.clear();
    current = messages.begin();
    BufferBase::set(nullptr, 0, 0);
}


ReadBufferFromRabbitMQConsumer::~ReadBufferFromRabbitMQConsumer()
{
    unsubscribe(); /// actually there is no need to unsubscribe because closing a channel will do the same

    messages.clear();
    current = messages.begin();
    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromRabbitMQConsumer::initQueueBindings(const String & exchange_name, const Names & routing_keys)
{
    bool bindings_ok = false, bindings_error = false;

    consumer_channel->declareExchange(exchange_name, AMQP::direct).onError([&](const char * message)
    {
        bindings_error = true;
        LOG_ERROR(log, "Failed to declare exchange: " << message);
    });

    consumer_channel->declareQueue(queue_name, AMQP::exclusive)
        .onSuccess([&](const std::string & /* queue_name */, int /* msgcount */, int /* consumercount */)
        {
            for (auto & key : routing_keys)
            {
                consumer_channel->bindQueue(exchange_name, "", key)
                .onSuccess([&]
                {
                    bindings_ok = true;
                })
                .onError([&](const char * message)
                {
                    bindings_error = true;
                    LOG_ERROR(log, "Failed to create queue binding: " << message);
                });
            }
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
    bool consumer_ok = false, consumer_error = false;

    consumer_channel->consume(queue_name, AMQP::noack)
        .onSuccess([&](const std::string &consumer)
        {
            if (consumerTag == "")
                consumerTag = consumer;

            consumer_ok = true;
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

            //stopEventLoop();
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

        messages = std::move(received);
        current = messages.begin();

        if (messages.empty())
        {
            LOG_TRACE(log, "Stalled");
            stalled = true;

            return false;
        }
    }

    auto new_position = const_cast<char *>(current->data());
    BufferBase::set(new_position, current->size(), 0);

    ++current;

    return true;
}

}
