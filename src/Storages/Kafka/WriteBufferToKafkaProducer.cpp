#include "WriteBufferToKafkaProducer.h"
#include "Core/Block.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"

#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event KafkaRowsWritten;
    extern const Event KafkaProducerFlushes;
    extern const Event KafkaMessagesProduced;
    extern const Event KafkaProducerErrors;
}

namespace DB
{
WriteBufferToKafkaProducer::WriteBufferToKafkaProducer(
    ProducerPtr producer_,
    const std::string & topic_,
    std::optional<char> delimiter,
    size_t rows_per_message,
    size_t chunk_size_,
    std::chrono::milliseconds poll_timeout,
    const Block & header
    )
    : WriteBuffer(nullptr, 0)
    , producer(producer_)
    , topic(topic_)
    , delim(delimiter)
    , max_rows(rows_per_message)
    , chunk_size(chunk_size_)
    , timeout(poll_timeout)
{
    if (header.has("_key"))
    {
        auto column_index = header.getPositionByName("_key");
        auto column_info = header.getByPosition(column_index);
        if (isString(column_info.type))
        {
            key_column_index = column_index;
        }
        // else ? (not sure it's a good place to report smth to user)
    }

    if (header.has("_timestamp"))
    {
        auto column_index = header.getPositionByName("_timestamp");
        auto column_info = header.getByPosition(column_index);
        if (isDateTime(column_info.type))
        {
            timestamp_column_index = column_index;
        }
    }

    reinitializeChunks();
}

WriteBufferToKafkaProducer::~WriteBufferToKafkaProducer()
{
    assert(rows == 0);
}

void WriteBufferToKafkaProducer::countRow(const Columns & columns, size_t current_row)
{
    ProfileEvents::increment(ProfileEvents::KafkaRowsWritten);

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

        cppkafka::MessageBuilder builder(topic);
        builder.payload(payload);

        // Note: if it will be few rows per message - it will take the value from last row of block
        if (key_column_index)
        {
            const auto & key_column = assert_cast<const ColumnString &>(*columns[key_column_index.value()]);
            const auto key_data = key_column.getDataAt(current_row);
            builder.key(cppkafka::Buffer(key_data.data, key_data.size));
        }

        if (timestamp_column_index)
        {
            const auto & timestamp_column = assert_cast<const ColumnUInt32 &>(*columns[timestamp_column_index.value()]);
            const auto timestamp = std::chrono::seconds{timestamp_column.getElement(current_row)};
            builder.timestamp(timestamp);
        }

        while (true)
        {
            try
            {
                producer->produce(builder);
            }
            catch (cppkafka::HandleException & e)
            {
                if (e.get_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL)
                {
                    producer->poll(timeout);
                    continue;
                }
                ProfileEvents::increment(ProfileEvents::KafkaProducerErrors);
                throw;
            }
            ProfileEvents::increment(ProfileEvents::KafkaMessagesProduced);

            break;
        }

        reinitializeChunks();
    }
}

void WriteBufferToKafkaProducer::flush()
{
    // For unknown reason we may hit some internal timeout when inserting for the first time.
    while (true)
    {
        try
        {
            producer->flush(timeout);
        }
        catch (cppkafka::HandleException & e)
        {
            if (e.get_error() == RD_KAFKA_RESP_ERR__TIMED_OUT)
                continue;

            ProfileEvents::increment(ProfileEvents::KafkaProducerErrors);
            throw;
        }

        ProfileEvents::increment(ProfileEvents::KafkaProducerFlushes);
        break;
    }
}

void WriteBufferToKafkaProducer::nextImpl()
{
    addChunk();
}

void WriteBufferToKafkaProducer::addChunk()
{
    chunks.push_back(std::string());
    chunks.back().resize(chunk_size);
    set(chunks.back().data(), chunk_size);
}

void WriteBufferToKafkaProducer::reinitializeChunks()
{
    rows = 0;
    chunks.clear();
    /// We cannot leave the buffer in the undefined state (i.e. without any
    /// underlying buffer), since in this case the WriteBuffeR::next() will
    /// not call our nextImpl() (due to available() == 0)
    addChunk();
}

}
