#include "WriteBufferToKafkaProducer.h"
#include "Core/Block.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"

namespace DB
{
WriteBufferToKafkaProducer::WriteBufferToKafkaProducer(
    ProducerPtr producer_,
    const std::string & topic_,
    std::optional<char> delimiter,
    size_t rows_per_message,
    size_t chunk_size_,
    std::chrono::milliseconds poll_timeout,
    Block header
    )
    : WriteBuffer(nullptr, 0)
    , producer(producer_)
    , topic(topic_)
    , delim(delimiter)
    , max_rows(rows_per_message)
    , chunk_size(chunk_size_)
    , timeout(poll_timeout)
{
    for (size_t i = 0; i < header.columns(); ++i)
    {
        auto column_info = header.getByPosition(i);

        if (column_info.name == "_key" && isString(column_info.type) )
        {
            key_column_index = i;
        }
        else if (column_info.name == "_timestamp" && isDateTime(column_info.type)) {
            timestamp_column_index = i;
        }
    }


}

WriteBufferToKafkaProducer::~WriteBufferToKafkaProducer()
{
    assert(rows == 0 && chunks.empty());
}

void WriteBufferToKafkaProducer::count_row(const Columns & columns, size_t current_row)
{

    if (++rows % max_rows == 0)
    {
        std::string payload;
        payload.reserve((chunks.size() - 1) * chunk_size + offset());
        for (auto i = chunks.begin(), e = --chunks.end(); i != e; ++i)
            payload.append(*i);
        int trunk_delim = delim && chunks.back()[offset() - 1] == delim ? 1 : 0;
        payload.append(chunks.back(), 0, offset() - trunk_delim);

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
                throw e;
            }

            break;
        }

        rows = 0;
        chunks.clear();
        set(nullptr, 0);
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
            throw e;
        }

        break;
    }
}

void WriteBufferToKafkaProducer::nextImpl()
{
    chunks.push_back(std::string());
    chunks.back().resize(chunk_size);
    set(chunks.back().data(), chunk_size);
}

}
