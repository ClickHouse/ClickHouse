#include <utility>

#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>
#include <common/logger_useful.h>
#include <amqpcpp.h>

namespace DB
{

ReadBufferFromRabbitMQConsumer::ReadBufferFromRabbitMQConsumer(
        ChannelPtr channel_,
        Poco::Logger * log_,
        size_t max_batch_size,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , consumer_channel(std::move(channel_))
        , log(log_)
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
            LOG_TRACE(log, "Channel is closed");
        });
}


ReadBufferFromRabbitMQConsumer::~ReadBufferFromRabbitMQConsumer()
{
    unsubscribe();

    messages.clear();
    current = messages.begin();
    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromRabbitMQConsumer::subscribe(const String & exchange_name, const Names & routing_keys)
{
    // consumer_channel->setQos(batch_size, false);
        
    for (auto key : routing_keys)
    {
        LOG_DEBUG(log, "Attempt to subscribe to - " + key);

        consumer_channel->declareExchange(exchange_name, AMQP::direct).onError([&](const char * message)
        {
            LOG_TRACE(log, "Exchange error - " << message);
        });

        /// since we let the library generate consumerTag, this is the only way to access it - we save it for the future
        if (consumerTag == "")
        {
            /* queue.declare is an idempotent operation. So, if you run it once, twice, N times, the result
             * will still be the same. We need to ensure that the queue exists before using it. */
            consumer_channel->declareQueue(AMQP::exclusive)
                .onSuccess([&](const std::string &queue_name, int /*msgcount*/, int /*consumercount*/)
                {
                    LOG_TRACE(log, "Queue declared with the binding key - "+ key);

                    consumer_channel->bindQueue(exchange_name, "", key);
                    consumer_channel->consume(queue_name, AMQP::noack)
                    .onSuccess([&](const std::string &consumer)
                    {
                        LOG_TRACE(log, "Consumer is open and successfully subscribed by key - " + key);

                        consumerTag = consumer;
                    })
                    .onReceived([&](const AMQP::Message & message, uint64_t deliveryTag, bool redelivered)
                    {
                        LOG_DEBUG(log, "Message reseived: " << message.body());

                        messages.push_back(RabbitMQMessage(const_cast<char *>(message.body()), message.bodySize(),
                        message.exchange(), message.routingkey(), deliveryTag, redelivered));
                        this->stalled = false; 
                    })
                    .onError([&](const char * message)
                    {
                        LOG_ERROR(log, "Failed to create consumer - " << message);
                    });

                })
                .onError([&](const char *message)
                {
                    LOG_ERROR(log, "Failed to declare queue on the channel - " << message);
                });
        }
        else
        {
            consumer_channel->declareQueue(AMQP::exclusive)
                .onSuccess([&](const std::string &queue_name, int /*msgcount*/, int /*consumercount*/)
                {
                    consumer_channel->bindQueue(exchange_name, "", key);
                    consumer_channel->consume(queue_name, AMQP::noack)
                    .onSuccess([&](const std::string & /* consumer */)
                    {
                        LOG_TRACE(log, "Consumer is open and successfully subscribed by key - " + key);
                    })
                    .onReceived([&](const AMQP::Message & message, uint64_t deliveryTag, bool redelivered)
                    {
                        LOG_DEBUG(log, "Message reseived: " << message.body());

                        messages.push_back(RabbitMQMessage(const_cast<char *>(message.body()), message.bodySize(),
                        message.exchange(), message.routingkey(), deliveryTag, redelivered));
                        this->stalled = false; 
                    })
                    .onError([&](const char *message)
                    {
                        LOG_ERROR(log, "Failed to create consumer - " << message);
                    });
                })
                .onError([&](const char * message)
                {
                    LOG_ERROR(log, "Failed to declare queue on the channel - " << message);
                });
        }
    }
}


void ReadBufferFromRabbitMQConsumer::unsubscribe()
{
    LOG_DEBUG(log, "Unsubscribing consumer");

    if (consumer_channel->usable() && consumerTag != "")
    {
        consumer_channel->cancel(consumerTag);
    }
}


void ReadBufferFromRabbitMQConsumer::start_consuming(RabbitMQHandler & handler)
{
    LOG_TRACE(log, "Consumer started consuming...");
    handler.process();
}


///TODO:The methods below are not done yet
//
/* Having the server PUSH messages to the client is one of the two ways to get messages
to the client, and also the preferred one. This is known as consuming messages via a subscription.
(The alternative is for the client to poll for messages one at a time, over the channel, via a get method.)
Since messages are pushed and are not to be pulled - no explicit fetch (commit) is required. (Consume method and handler->process() is enough.)
So this method is called only in streamToViews(), where no subcription took place. */
void ReadBufferFromRabbitMQConsumer::commitNotSubscribed(const Names & routing_keys)
{
    LOG_DEBUG(log, "CommitNotSubscribed.");

    consumer_channel->setQos(batch_size, false); /// per consumer limit. FIXME: size_t may not fit into uint16_t

    for (auto & key : routing_keys)
    {
        consumer_channel->consume(key, consumerTag, AMQP::noack)
            .onReceived([](const AMQP::Message & /* message */, uint64_t /* deliveryTag */, bool /* redelivered */)
            {
                /// save messages
            }).onError([this](const char * message)
            {
                LOG_TRACE(log, message);
            });
    }

    stalled = false;
}


/// A possible alternative to commitNotSubscribed(). (Not used - probably will be deleted.)
void ReadBufferFromRabbitMQConsumer::commitViaGet(const Names & routing_keys)
{
    auto key = routing_keys.begin();
    size_t count_fetched = 0;
    bool stop = 0;

    while (count_fetched < batch_size && key != routing_keys.end() && !stop)
    {
        consumer_channel->get(*key, AMQP::multiple).onSuccess(
                [&count_fetched](const AMQP::Message & /* message */, uint64_t /* deliveryTag */, bool /* redelivered */)
                {
                    ++count_fetched;

                }).onError([this, &stop](const char * message)
                {
                    LOG_TRACE(log, message);
                    stop = 1;
                });

        if (count_fetched < batch_size)
            ++key;
    }

    stalled = false;
}


bool ReadBufferFromRabbitMQConsumer::nextImpl()
{
    if (!allowed || stalled || stopped)
        return false;

    /* Messages list is filled in consume(...).onReceived() callback method (above) - once a message is pushed to consumer.
    Since messages are pushed and not pulled, Messages list would not normally be empty if at least one message was successfully sent */
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
