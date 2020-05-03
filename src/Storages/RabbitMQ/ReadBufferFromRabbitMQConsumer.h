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
            size_t max_batch_size, 
            const std::atomic<bool> & stopped_);
    ~ReadBufferFromRabbitMQConsumer() override;

    void allowNext() { allowed = true; } // Allow to read next message.
    void initQueueBindings(const String & exchange_name, const Names & routing_keys);
    void subscribe();
    void unsubscribe();
    void flush();

    void startEventLoop();
    void startNonBlockEventLoop();
    void stopEventLoop();

    bool getStalled() { return stalled; }
    bool getCnt() { return cnt; }

private:
    struct RabbitMQMessage
    {
        String message;
        size_t size;

        RabbitMQMessage(String message_, size_t size_) : message(message_), size(size_) {}
    };

    using Messages = std::vector<RabbitMQMessage>;

    ChannelPtr consumer_channel;
    RabbitMQHandler & eventHandler;
    Poco::Logger * log;
    char row_delimiter;
    const size_t batch_size = 1;
    bool allowed = true, stalled = false;
    const std::atomic<bool> & stopped;

    String consumerTag; // ID for the consumer
    const String queue_name = "ClickHouseRabbitMQQueue";

    size_t cnt = 0;

    size_t cnt_2 = 0;

    Messages messages;
    Messages::iterator current;

    bool nextImpl() override;
};
}
