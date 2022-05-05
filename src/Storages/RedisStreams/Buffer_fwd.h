#pragma once

#include <memory>

namespace DB
{

class ReadBufferFromRedisStreams;
class WriteBufferToRedisStreams;

using ConsumerBufferPtr = std::shared_ptr<ReadBufferFromRedisStreams>;
using ProducerBufferPtr = std::shared_ptr<WriteBufferToRedisStreams>;

}
