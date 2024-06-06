#include "KafkaProducer.h"
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

KafkaProducer::KafkaProducer(
    ProducerPtr producer_, const std::string & topic_, std::chrono::milliseconds poll_timeout, std::atomic<bool> & shutdown_called_, const Block & header)
    : IMessageProducer(getLogger("KafkaProducer"))
    , producer(producer_)
    , topic(topic_)
    , timeout(poll_timeout)
    , shutdown_called(shutdown_called_)
{
    if (header.has("_key"))
    {
        auto column_index = header.getPositionByName("_key");
        const auto & column_info = header.getByPosition(column_index);
        if (isString(column_info.type))
            key_column_index = column_index;
        // else ? (not sure it's a good place to report smth to user)
    }

    if (header.has("_timestamp"))
    {
        auto column_index = header.getPositionByName("_timestamp");
        const auto & column_info = header.getByPosition(column_index);
        if (isDateTime(column_info.type))
            timestamp_column_index = column_index;
    }
}

void KafkaProducer::produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row)
{
    ProfileEvents::increment(ProfileEvents::KafkaRowsWritten, rows_in_message);
    cppkafka::MessageBuilder builder(topic);
    builder.payload(message);

    // Note: if it will be few rows per message - it will take the value from last row of block
    if (key_column_index)
    {
        const auto & key_column = assert_cast<const ColumnString &>(*columns[key_column_index.value()]);
        const auto key_data = key_column.getDataAt(last_row);
        builder.key(cppkafka::Buffer(key_data.data, key_data.size));
    }

    if (timestamp_column_index)
    {
        const auto & timestamp_column = assert_cast<const ColumnUInt32 &>(*columns[timestamp_column_index.value()]);
        const auto timestamp = std::chrono::seconds{timestamp_column.getElement(last_row)};
        (void)builder.timestamp(timestamp);
    }

    while (!shutdown_called)
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
}

void KafkaProducer::finish()
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

}
