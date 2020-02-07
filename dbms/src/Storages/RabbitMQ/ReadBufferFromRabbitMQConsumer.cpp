#include <utility>

#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>
#include <common/logger_useful.h>
#include <amqpcpp.h>

namespace DB
{

ReadBufferFromRabbitMQConsumer::ReadBufferFromRabbitMQConsumer(
        ChannelPtr consumer_channel_,
        Poco::Logger * log_,
        size_t max_batch_size,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(consumer_channel_))
        , log(log_)
        , batch_size(max_batch_size)
        , stopped(stopped_)
        , consumerTag("")
        , current(messages.begin())
{
    messages.reserve(batch_size);
    consumer_channel->setQos(batch_size, false); //FIXME: admitted limit is uint16_t, size_t may not fit
}

ReadBufferFromRabbitMQConsumer::~ReadBufferFromRabbitMQConsumer()
{
    unsubscribe();
}

void ReadBufferFromRabbitMQConsumer::subscribe(const Names & routing_keys)
{
    for (auto key : routing_keys)
    {
        /* queue.declare is an idempotent operation. So, if you run it once, twice, N times, the result
         * will still be the same. We need to ensure that the queue exists before using it. */
        consumer_channel->declareQueue(key);

        /// Tell the RabbitMQ server that we're ready to consume messages from queue with the given key
        if (consumerTag == "")
        {
            /// since we let the library generate consumerTag, this is the only way to access it
            consumer_channel->consume(key, AMQP::noack).onSuccess([this](const std::string &consumer)
            {
                   consumerTag = consumer;

            }).onReceived([this](const AMQP::Message & message, uint64_t deliveryTag, bool redelivered)
            {
                messages.push_back(RabbitMQMessage(const_cast<char *>(message.body()), message.bodySize(),
                        message.exchange(), message.routingkey(), deliveryTag, redelivered));

            }).onError([this](const char *message)
            {
                LOG_TRACE(log, message);
            });
        }
        else
        {
            consumer_channel->consume(key, consumerTag, AMQP::noack).onReceived(
                    [this](const AMQP::Message & message, uint64_t deliveryTag, bool redelivered)
           {
               messages.push_back(RabbitMQMessage(const_cast<char *>(message.body()), message.bodySize(),
                       message.exchange(), message.routingkey(), deliveryTag, redelivered));

           }).onError([this](const char *message)
           {
               LOG_TRACE(log, message);
           });
        }

        if (!nextImpl())  // Not sure whether this func should be called here
        {
        }

        stalled = false;
    }
}

void ReadBufferFromRabbitMQConsumer::unsubscribe()
{
    if (consumer_channel->usable())
    {
        consumer_channel->cancel(consumerTag);
        consumer_channel->close();
    }

    messages.clear();
    current = messages.begin();
    BufferBase::set(nullptr, 0, 0);
}

bool ReadBufferFromRabbitMQConsumer::nextImpl()
{
    if (!allowed || stalled || stopped)
        return false;

    /// Since a message is pushed to consumer once it is ready, messages list would not normally be empty
    if (messages.empty())
    {
        LOG_TRACE(log, "Stalled");
        stalled = true;
        return false;
    }

    auto new_position = current->message;
    BufferBase::set(new_position, current->size, 0);
    allowed = false;

    ++current;

    if (current == messages.end())
    {
        /// TODO: something has to be done - messages should not be dropped immediately,
        /// since we may need them for virtual columns.
        messages.clear();
        current = messages.begin();
    }

    return true;
}

}
