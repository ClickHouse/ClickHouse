#pragma once

#include <IO/WriteBuffer.h>

#include <cppkafka/cppkafka.h>

namespace DB {

class WriteBufferToKafkaProducer;

using ProducerBufferPtr = std::shared_ptr<WriteBufferToKafkaProducer>;
using ProducerPtr = std::shared_ptr<cppkafka::Producer>;

class WriteBufferToKafkaProducer : public WriteBuffer
{
public:
    WriteBufferToKafkaProducer(
        ProducerPtr producer_,
        const std::string & topic_,
        size_t rows_per_message,
        size_t chunk_size_,
        std::chrono::milliseconds poll_timeout);
    ~WriteBufferToKafkaProducer() override;

    void count_row();
    void flush();

private:
    void nextImpl() override;

    ProducerPtr producer;
    const std::string topic;
    const size_t max_rows;
    const size_t chunk_size;
    const std::chrono::milliseconds timeout;

    size_t rows = 0;
    std::list<std::string> chunks;
};

} // namespace DB
