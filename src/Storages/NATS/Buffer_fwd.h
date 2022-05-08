#pragma once

#include <memory>

namespace DB
{

class ReadBufferFromNATSConsumer;
using ConsumerBufferPtr = std::shared_ptr<ReadBufferFromRabbitMQConsumer>;

class WriteBufferToNATSProducer;
using ProducerBufferPtr = std::shared_ptr<WriteBufferToRabbitMQProducer>;

}
