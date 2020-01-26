#pragma once
#include <Core/Names.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>

#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

using ConsumerPtr = std::shared_ptr<AMQP::Channel>;

class ReadBufferFromRabbitMQConsumer : public ReadBuffer
{
public:
    ReadBufferFromRabbitMQConsumer(
            ConsumerPtr consumer_,
            RabbitMQHandler * handler_);
    ~ReadBufferFromRabbitMQConsumer() override;

    void allowNext() { allowed = true; } // Allow to read next message.
    void commit(); // Commit all processed messages.

    void subscribe(const Names & topics);
    void unsubscribe();


private:
    using Messages = std::vector<AMQP::Message>;

    ConsumerPtr consumer;
    RabbitMQHandler * handler;
    bool allowed = true;

    Messages messages;
    Messages::const_iterator current;

};
}