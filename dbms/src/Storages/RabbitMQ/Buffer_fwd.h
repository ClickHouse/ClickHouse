#pragma once

#include <memory>

namespace DB
{

    class ReadBufferFromRabbitMQConsumer;
    class WriteBufferToRabbitMQProducer;

    using ConsumerBufferPtr = std::shared_ptr<ReadBufferFromRabbitMQConsumer>;
    using ProducerBufferPtr = std::shared_ptr<WriteBufferToRabbitMQProducer>;

}