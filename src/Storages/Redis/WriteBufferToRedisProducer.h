#pragma once

#include <IO/WriteBuffer.h>
#include <Columns/IColumn.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <redis++/redis++.h>
#include <boost/algorithm/string.hpp>

#include <cppkafka/cppkafka.h>

#include <list>

namespace DB
{
class Block;
using ProducerPtr = std::shared_ptr<cppkafka::Producer>;
using RedisPtr = std::shared_ptr<sw::redis::Redis>;

class WriteBufferToRedisProducer : public WriteBuffer
{
public:
    WriteBufferToRedisProducer(
        RedisPtr redis_,
        const std::string & stream_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_);
    ~WriteBufferToRedisProducer() override;

    void countRow();

private:
    void nextImpl() override;
    void addChunk();
    void reinitializeChunks();
    static std::vector<std::pair<std::string, std::string>> convertRawPayloadToItems(const std::string& payload);

    RedisPtr redis;
    const std::string stream;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;

    size_t rows = 0;
    std::list<std::string> chunks;
    std::optional<size_t> timestamp_column_index;
};

}
