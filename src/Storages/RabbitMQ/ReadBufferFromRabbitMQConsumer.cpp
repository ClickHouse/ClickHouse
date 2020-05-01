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
        size_t max_batch_size,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(channel_))
        , eventHandler(eventHandler_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , batch_size(max_batch_size)
        , stopped(stopped_)
        , consumerTag("")
        , current(messages.begin())
{
    messages.reserve(batch_size);

    consumer_channel->confirmSelect()
        .onSuccess([&]()
        {
            LOG_DEBUG(log, "Channel is successfully open");
        })
        .onError([&](const char * message)
        {
            LOG_ERROR(log, "Error with the consumer channel - " << message);

            /// TODO: if a consumer needs to be restored, make a new channel here
        })
        .onFinalize([&]()
        {
            LOG_DEBUG(log, "Channel is closed");
        });

    messages.clear();
    current = messages.begin();
    BufferBase::set(nullptr, 0, 0);
}


ReadBufferFromRabbitMQConsumer::~ReadBufferFromRabbitMQConsumer()
{
    //unsubscribe();

    messages.clear();
    current = messages.begin();
    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromRabbitMQConsumer::initQueueBindings(const String & exchange_name, const Names & routing_keys)
{
    consumer_channel->declareExchange(exchange_name, AMQP::direct).onError([&](const char * message)
    {
        LOG_ERROR(log, "Exchange error - " << message);
        eventHandler.stop();
    });

    consumer_channel->declareQueue("RabbitMQQueue", AMQP::exclusive)
        .onSuccess([&](const std::string & /* queue_name */, int /* msgcount */, int /* consumercount */)
        {
            for (auto & key : routing_keys)
            {
                consumer_channel->bindQueue(exchange_name, "", key)
                .onSuccess([&]
                {
                    LOG_TRACE(log, "Queue declared and bound to key.");
                    stopEventLoop();
                })
                .onError([&](const char * message)
                {
                    LOG_ERROR(log, "Failed to create queue binding: " << message);
                    stopEventLoop();
                });
            }
    })
    .onError([&](const char * message)
    {
        LOG_ERROR(log, "Failed to declare queue on the channel - " << message);
        stopEventLoop();
    });

    startEventLoop();
}


void ReadBufferFromRabbitMQConsumer::subscribe()
{
    consumer_channel->consume(queue_name, AMQP::noack)
    .onSuccess([&](const std::string &consumer)
    {
        LOG_TRACE(log, "Consumer is open");
        if (consumerTag == "")
            consumerTag = consumer;

            stopEventLoop();
    })
    .onReceived([&](const AMQP::Message & message, uint64_t deliveryTag, bool redelivered)
    {
        char * mes = const_cast<char *>(message.body());
        mes[message.bodySize()] = '\0';

        String message_received = std::string(message.body(), message.body() + message.bodySize());
        size_t message_size = message.bodySize();

        if (row_delimiter != '\0')
        {
            message_received += row_delimiter;
            message_size += 1;
        }

        messages.emplace_back(RabbitMQMessage(message_received, message_size,
        message.exchange(), message.routingkey(), deliveryTag, redelivered));

        this->stalled = false; 
        stopEventLoop();
    })
    .onError([&](const char * message)
    {
        LOG_ERROR(log, "Failed to create consumer - " << message);
        stopEventLoop();
    });

    startEventLoop();
}


void ReadBufferFromRabbitMQConsumer::unsubscribe()
{
    LOG_DEBUG(log, "Unsubscribing consumer");

    if (consumer_channel->usable() && consumerTag != "")
    {
        consumer_channel->cancel(consumerTag);
    }
}


void ReadBufferFromRabbitMQConsumer::startEventLoop()
{
    eventHandler.start();
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
        size_t prev_size = messages.size();
        startNonBlockEventLoop();

        if (messages.size() == prev_size)
        {
            LOG_TRACE(log, "Stalled");
            stalled = true;
            return false;
        }
    }

    auto new_position = const_cast<char *>(current->message.c_str());
    BufferBase::set(new_position, current->size, 0);
    allowed = false;

    ++current;

    return true;
}

}
