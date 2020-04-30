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
            size_t max_batch_size,
            const std::atomic<bool> & stopped_);
    ~ReadBufferFromRabbitMQConsumer() override;

    void allowNext() { allowed = true; } // Allow to read next message.
    void subscribe(const String & exchange_name, const Names & routing_keys);
    void unsubscribe();

    void startEventLoop();
    void startNonBlockEventLoop();
    void stopEventLoop();

    void commitNotSubscribed(const Names & routing_keys);
    void commitViaGet(const Names & routing_keys);

    String getCurrentExchange() const { return current[-1].exchange; }
    String getCurrentRoutingKey() const { return current[-1].routingKey; }
    UInt64 getCurrentDeliveryTag() const { return current[-1].deliveryTag; }

private:
    struct RabbitMQMessage
    {
        String message;
        size_t size;
        String exchange;
        String routingKey;
        UInt64 deliveryTag;
        bool redelivered;

        RabbitMQMessage(
                String message_, size_t size_, String exchange_, String routingKey_,
                UInt64 deliveryTag_, bool redelivered_) :
                message(message_), size(size_), exchange(exchange_), routingKey(routingKey_),
                deliveryTag(deliveryTag_), redelivered(redelivered_) {}

    };

   // using Messages = std::vector<RabbitMQMessage>;

    ChannelPtr consumer_channel;
    RabbitMQHandler & eventHandler;
    Poco::Logger * log;
    const size_t batch_size = 1;
    bool allowed = true, stalled = false;
    const std::atomic<bool> & stopped;

    int cnt = 0;

    String consumerTag; // ID for the consumer

    std::vector<RabbitMQMessage>  messages;
    std::vector<RabbitMQMessage>::const_iterator current;

    bool nextImpl() override;
};
}
