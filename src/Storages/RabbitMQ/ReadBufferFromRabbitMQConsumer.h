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
            Poco::Logger * log_,
            char row_delimiter_,
            const std::atomic<bool> & stopped_);
    ~ReadBufferFromRabbitMQConsumer() override;

    void initQueueBindings(const String & exchange_name, const Names & routing_keys);
    void subscribe();
    void unsubscribe();
    void flush();

    void startEventLoop();
    void startNonBlockEventLoop();
    void stopEventLoop();

private:
    using Messages = std::vector<String>;

    ChannelPtr consumer_channel;
    RabbitMQHandler & eventHandler;

    Poco::Logger * log;
    char row_delimiter;
    bool stalled = false;
    const std::atomic<bool> & stopped;

    String consumerTag; // ID for the consumer
    const String queue_name = "ClickHouseRabbitMQQueue";

    Messages received;
    Messages messages;
    Messages::iterator current;

    bool nextImpl() override;
};
}
