#include "WriteBufferToRedisProducer.h"
#include "Core/Block.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"

namespace DB
{
WriteBufferToRedisProducer::WriteBufferToRedisProducer(
    RedisPtr redis_,
    const std::string & stream_,
    std::optional<char> delimiter,
    size_t rows_per_message,
    size_t chunk_size_
    )
    : WriteBuffer(nullptr, 0)
    , redis(redis_)
    , stream(stream_)
    , delim(delimiter)
    , max_rows(rows_per_message)
    , chunk_size(chunk_size_)
{
    reinitializeChunks();
}

WriteBufferToRedisProducer::~WriteBufferToRedisProducer()
{
    assert(rows == 0);
}

void WriteBufferToRedisProducer::countRow()
{
    if (++rows % max_rows == 0)
    {
        const std::string & last_chunk = chunks.back();
        size_t last_chunk_size = offset();

        // if last character of last chunk is delimiter - we don't need it
        if (last_chunk_size && delim && last_chunk[last_chunk_size - 1] == delim)
            --last_chunk_size;

        std::string payload;
        payload.reserve((chunks.size() - 1) * chunk_size + last_chunk_size);

        // concat all chunks except the last one
        for (auto i = chunks.begin(), e = --chunks.end(); i != e; ++i)
            payload.append(*i);

        // add last one
        payload.append(last_chunk, 0, last_chunk_size);

        std::vector<std::pair<std::string, std::string>> items = convertRawPayloadToItems(payload);
        redis->xadd(stream, "*", items.begin(), items.end());

        reinitializeChunks();
    }
}

std::vector<std::pair<std::string, std::string>> WriteBufferToRedisProducer::convertRawPayloadToItems(const std::string& payload)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(payload);
    Poco::DynamicStruct dict = *json.extract<Poco::JSON::Object::Ptr>();

    std::vector<std::pair<std::string, std::string>> result;
    result.reserve(dict.size());
    for (auto& [key, val] : dict)
    {
        result.emplace_back(key, std::move(val));
    }

    return result;
}

void WriteBufferToRedisProducer::nextImpl()
{
    addChunk();
}

void WriteBufferToRedisProducer::addChunk()
{
    chunks.push_back(std::string());
    chunks.back().resize(chunk_size);
    set(chunks.back().data(), chunk_size);
}

void WriteBufferToRedisProducer::reinitializeChunks()
{
    rows = 0;
    chunks.clear();
    /// We cannot leave the buffer in the undefined state (i.e. without any
    /// underlying buffer), since in this case the WriteBuffeR::next() will
    /// not call our nextImpl() (due to available() == 0)
    addChunk();
}

}
