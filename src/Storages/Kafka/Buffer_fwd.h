#pragma once

#include <memory>

namespace DB
{

class ReadBufferFromKafkaConsumer;
class WriteBufferToKafkaProducer;

using ConsumerBufferPtr = std::shared_ptr<ReadBufferFromKafkaConsumer>;
using ProducerBufferPtr = std::shared_ptr<WriteBufferToKafkaProducer>;

}
