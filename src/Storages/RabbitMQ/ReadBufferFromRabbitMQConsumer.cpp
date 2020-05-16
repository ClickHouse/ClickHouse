#include <utility>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <common/logger_useful.h>
#include <amqpcpp.h>


namespace DB
{

ReadBufferFromRabbitMQConsumer::ReadBufferFromRabbitMQConsumer(
        std::pair<std::string, UInt16> & parsed_address,
        const String & exchange_name_,
        const String & routing_key_,
        Poco::Logger * log_,
        char row_delimiter_,
        const bool hash_exchange_,
        const size_t num_queues_,
        const std::atomic<bool> & stopped_)
        : ReadBuffer(nullptr, 0)
        , evbase(event_base_new())
        , eventHandler(evbase, log)
        , connection(&eventHandler, 
          AMQP::Address(parsed_address.first, parsed_address.second, AMQP::Login("root", "clickhouse"), "/"))
        , exchange_name(exchange_name_)
        , routing_key(routing_key_)
        , log(log_)
        , row_delimiter(row_delimiter_)
        , hash_exchange(hash_exchange_)
        , num_queues(num_queues_)
        , stopped(stopped_)
{
    /* It turned out to be very important to make a different connection each time the object of this class is created,
     * because in case when num_consumers > 1 - inputStreams run asynchronously and if they share the same connection,
     * then they also will share the same event loop. But it will mean that if one stream's consumer starts event loop,
     * then it will run all callbacks on the connection - including other stream's consumer's callbacks - 
     * it result in asynchronous run of the same code and lead to a lot of seg faults.
     */
    while (!connection.ready())
    {
        event_base_loop(evbase, EVLOOP_NONBLOCK | EVLOOP_ONCE);
    }

    consumer_channel = std::make_shared<AMQP::TcpChannel>(&connection);

    messages.clear();
    current = messages.begin();

    /* One queue per consumer can handle up to 50000 messages. More queues per consumer can be added.
     * By default there is one queue per consumer.
     */
    for (size_t i = 0; i < num_queues; ++i)
    {
        initQueueBindings();
    }

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
    /* As there are 5 different types of exchanges and the type should be set as a parameter while publishing the message,
     * then for uniformity this parameter should always be set as fanout-exchange type. In current implementation, the exchange,
     * to which messages a published, will be bound to the exchange of the needed type, which will distribute messages according to its type.
     */
    consumer_channel->declareExchange(exchange_name, AMQP::fanout).onError([&](const char * message)
    {
        exchange_declared = false;
        LOG_ERROR(log, "Failed to declare fanout exchange: " << message);
    });

    if (hash_exchange)
    {
        /* If hash_exchange flag is set, it means that there should be a distribution of messages between multiple consumers
         * or queues - like rebalance. In this case a fanout exchange will pass all messages to  consistent-hash exchange, 
         * which will distribute messages (between queues) based on a hash value of the routing key. 
         * However, internally (with INSERT query) messages can be published directly to hash exchange.
         */
        current_exchange_name = exchange_name + "_hash";
        consumer_channel->declareExchange(current_exchange_name, AMQP::consistent_hash).onError([&](const char * message)
        {
            exchange_declared = false;
        });

        consumer_channel->bindExchange(exchange_name, current_exchange_name, routing_key).onError([&](const char * message)
        {
            exchange_declared = false;
        });
    }
    else
    {
        /// In simple cases with one queue and one consumer direct exchange type is used. 
        
        current_exchange_name = exchange_name + "_direct";
        consumer_channel->declareExchange(current_exchange_name, AMQP::direct).onError([&](const char * message)
        {
            exchange_declared = false;
        });

        consumer_channel->bindExchange(exchange_name, current_exchange_name, routing_key).onError([&](const char * message)
        {
            exchange_declared = false;
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

    consumer_channel->declareQueue(AMQP::exclusive + AMQP::autodelete)
    .onSuccess([&](const std::string &  queue_name_, int /* msgcount */, int /* consumercount */)
    {
        queues.emplace_back(queue_name_);
        LOG_TRACE(log, "Queue " + queue_name_ + " declared");

        consumer_channel->bindQueue(current_exchange_name, queue_name_, routing_key)
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

    if (bindings_ok)
    {
        bindings_created = true;
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
    bool consumer_ok = false, consumer_error = false;

    consumer_channel->consume(queue_name, AMQP::noack)
    .onSuccess([&](const std::string & consumer)
    {
        if (consumerTag == "")
            consumerTag = consumer;

        consumer_ok = true;

        LOG_TRACE(log, "Consumer " + consumerTag + " is subscribed to queue " + queue_name + " with the key " + routing_key);
    })
    .onReceived([&](const AMQP::Message & message, uint64_t /* deliveryTag */, bool /* redelivered */)
    {
        size_t message_size = message.bodySize();

        if (message_size && message.body() != nullptr)
        {
            String message_received = std::string(message.body(), message.body() + message_size);

            if (row_delimiter != '\0')
                message_received += row_delimiter;

            //LOG_TRACE(log, "Consumer " + consumerTag + " received the message " + message_received);

            received.push_back(message_received);
            this->stalled = false; 
        }
    })
    .onError([&](const char * message)
    {
        consumer_error = true;
        LOG_ERROR(log, "Consumer failed: " << message);
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
    if (stopped || !allowed)
        return false;

    if (current == messages.end())
    {
        if (received.empty())
        {
            startNonBlockEventLoop();
        }

        if (received.empty())
        {
            LOG_TRACE(log, "Stalled");
            stalled = true;

            return false;
        }

        messages.clear();
        messages.swap(received);
        current = messages.begin();
    }

    auto new_position = const_cast<char *>(current->data());
    BufferBase::set(new_position, current->size(), 0);

    ++current;
    allowed = false;

    return true;
}

}
