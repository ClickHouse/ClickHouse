#pragma once

#include <memory>

namespace DB
{

class ReadBufferFromRedisConsumer;
class WriteBufferToRedisProducer;

using ConsumerBufferPtr = std::shared_ptr<ReadBufferFromRedisConsumer>;
using ProducerBufferPtr = std::shared_ptr<WriteBufferToRedisProducer>;

}
