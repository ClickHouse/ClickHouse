#pragma once

#include <IO/WriteBuffer.h>
#include <Columns/IColumn.h>

#include <cppkafka/cppkafka.h>

#include <list>

#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric KafkaProducers;
}


namespace DB
{
class Block;
using ProducerPtr = std::shared_ptr<cppkafka::Producer>;

class WriteBufferToKafkaProducer : public WriteBuffer
{
public:
    WriteBufferToKafkaProducer(
        ProducerPtr producer_,
        const std::string & topic_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_,
        std::chrono::milliseconds poll_timeout,
        const Block & header);
    ~WriteBufferToKafkaProducer() override;

    void countRow(const Columns & columns, size_t row);
    void flush();

private:
    void nextImpl() override;
    void addChunk();
    void reinitializeChunks();
    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaProducers};

    ProducerPtr producer;
    const std::string topic;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;
    const std::chrono::milliseconds timeout;

    size_t rows = 0;
    std::list<std::string> chunks;
    std::optional<size_t> key_column_index;
    std::optional<size_t> timestamp_column_index;
};

}
