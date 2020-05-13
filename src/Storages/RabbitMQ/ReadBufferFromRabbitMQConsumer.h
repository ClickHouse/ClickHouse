#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <event2/event.h>


namespace Poco
{
    class Logger;
}


namespace DB
{

using ChannelPtr = std::shared_ptr<AMQP::TcpChannel>;

class ReadBufferFromRabbitMQConsumer : public ReadBuffer
{
public:
    ReadBufferFromRabbitMQConsumer(
            ChannelPtr channel_,
            RabbitMQHandler & eventHandler_,
            const String & exchange_name_,
            const String & routing_key_,
            Poco::Logger * log_,
            char row_delimiter_,
            const bool hash_exchange_,
            const size_t num_queues_,
            const std::atomic<bool> & stopped_);

    ~ReadBufferFromRabbitMQConsumer() override;

    void subscribeConsumer();

private:
    using Messages = std::vector<String>;
    using Queues = std::vector<String>;

    ChannelPtr consumer_channel;
    RabbitMQHandler & eventHandler;
    const String & exchange_name;
    const String & routing_key;

    Poco::Logger * log;
    char row_delimiter;
    const bool hash_exchange;
    bool stalled = false;
    const std::atomic<bool> & stopped;
    std::atomic<bool> exchange_declared = false;

    const size_t num_queues;
    String consumerTag; // ID for the consumer
    Queues queues;
    bool bindings_created = false;
    bool subscribed = false;
    //Queues::iterator queue_name;
    String hash_exchange_name;

    Messages received;
    Messages messages;
    Messages::iterator current;

    bool nextImpl() override;
    void initExchange();
    void initQueueBindings();
    void subscribe(const String & queue_name);
    void unsubscribe();

    void startNonBlockEventLoop();
    void stopEventLoop();
};
}
