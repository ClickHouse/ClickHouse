#pragma once

#include <memory>

namespace DB
{

class ReadBufferFromNATSConsumer;
using ConsumerBufferPtr = std::shared_ptr<ReadBufferFromNATSConsumer>;

class WriteBufferToNATSProducer;
using ProducerBufferPtr = std::shared_ptr<WriteBufferToNATSProducer>;

}
