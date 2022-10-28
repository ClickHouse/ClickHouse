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

static const auto BATCH = 1000;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

KafkaProducer::KafkaProducer(
    ProducerPtr producer_, const std::string & topic_, std::chrono::milliseconds poll_timeout, std::atomic<bool> & shutdown_called_, const Block & header)
    : producer(producer_), topic(topic_), timeout(poll_timeout), shutdown_called(shutdown_called_), payloads(BATCH)
{
    if (header.has("_key"))
    {
        auto column_index = header.getPositionByName("_key");
        auto column_info = header.getByPosition(column_index);
        if (isString(column_info.type))
            key_column_index = column_index;
        // else ? (not sure it's a good place to report smth to user)
    }

    if (header.has("_timestamp"))
    {
        auto column_index = header.getPositionByName("_timestamp");
        auto column_info = header.getByPosition(column_index);
        if (isDateTime(column_info.type))
            timestamp_column_index = column_index;
    }
}

void KafkaProducer::produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row)
{
    ProfileEvents::increment(ProfileEvents::KafkaRowsWritten, rows_in_message);
    Payload payload;
    payload.message = message;

    // Note: if it will be few rows per message - it will take the value from last row of block
    if (key_column_index)
    {
        const auto & key_column = assert_cast<const ColumnString &>(*columns[key_column_index.value()]);
        payload.key = key_column.getDataAt(last_row).toString();
    }

    if (timestamp_column_index)
    {
        const auto & timestamp_column = assert_cast<const ColumnUInt32 &>(*columns[timestamp_column_index.value()]);
        payload.timestamp = std::chrono::seconds{timestamp_column.getElement(last_row)};
    }

    if (!payloads.push(std::move(payload)))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
}


void KafkaProducer::producingTask()
{
    cppkafka::MessageBuilder builder(topic);
    while ((!payloads.isFinishedAndEmpty()) && !shutdown_called.load())
    {
        Payload payload;
        if (!payloads.pop(payload))
            break;

        builder.payload(payload.message);

        if (payload.key)
            builder.key(cppkafka::Buffer(payload.key->data(), payload.key->size()));

        if (payload.timestamp)
            builder.timestamp(*payload.timestamp);

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
    }
}

void KafkaProducer::stopProducingTask()
{
    payloads.finish();
}

void KafkaProducer::finishImpl()
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
