#pragma once

#include <memory>

namespace DB
{

class ReadBufferFromRabbitMQConsumer;
using ConsumerBufferPtr = std::shared_ptr<ReadBufferFromRabbitMQConsumer>;

}
