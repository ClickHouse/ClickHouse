#include <Storages/Kafka/KafkaProducer.h>
#include <Core/Block.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <Common/Logger.h>
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

namespace
{

/// Returns the index of a column with the given name if it exists and its type matches the `predicate`.
template <typename TypePredicate>
std::optional<size_t> findSpecialColumn(const Block & header, const std::string & name, TypePredicate predicate)
{
    if (!header.has(name))
        return std::nullopt;
    auto column_index = header.getPositionByName(name);
    const auto & column_info = header.getByPosition(column_index);
    if (!predicate(column_info.type))
        return std::nullopt;
    return column_index;
}

bool isStringArray(const DataTypePtr & type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
    if (!array_type)
        return false;
    return isString(array_type->getNestedType());
}

}

KafkaProducer::KafkaProducer(
    ProducerPtr producer_,
    const std::string & topic_,
    std::chrono::milliseconds poll_timeout,
    std::atomic<bool> & shutdown_called_,
    const Block & header,
    bool map_virtual_columns_on_write)
    : IMessageProducer(getLogger("KafkaProducer"))
    , producer(producer_)
    , topic(topic_)
    , timeout(poll_timeout)
    , shutdown_called(shutdown_called_)
{
    key_column_index = findSpecialColumn(header, "_key", [](const DataTypePtr & t) { return isString(t); });
    timestamp_column_index = findSpecialColumn(header, "_timestamp", [](const DataTypePtr & t) { return isDateTime(t); });

    if (map_virtual_columns_on_write)
    {
        headers_name_column_index = findSpecialColumn(header, "_headers.name", isStringArray);
        headers_value_column_index = findSpecialColumn(header, "_headers.value", isStringArray);
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
        builder.key(cppkafka::Buffer(key_data.data(), key_data.size()));
    }

    if (timestamp_column_index)
    {
        const auto & timestamp_column = assert_cast<const ColumnUInt32 &>(*columns[timestamp_column_index.value()]);
        const auto timestamp = std::chrono::seconds{timestamp_column.getElement(last_row)};
        (void)builder.timestamp(timestamp);
    }

    if (headers_name_column_index && headers_value_column_index)
    {
        const auto & names_array = assert_cast<const ColumnArray &>(*columns[headers_name_column_index.value()]);
        const auto & values_array = assert_cast<const ColumnArray &>(*columns[headers_value_column_index.value()]);

        const auto & names_data = assert_cast<const ColumnString &>(names_array.getData());
        const auto & values_data = assert_cast<const ColumnString &>(values_array.getData());

        const auto & names_offsets = names_array.getOffsets();
        const auto & values_offsets = values_array.getOffsets();

        const size_t names_start = last_row == 0 ? 0 : names_offsets[last_row - 1];
        const size_t names_end = names_offsets[last_row];
        const size_t values_start = last_row == 0 ? 0 : values_offsets[last_row - 1];

        /// `_headers.name` and `_headers.value` share the Nested prefix `_headers`, so
        /// `NestedElementsValidationTransform` guarantees both arrays have equal sizes here.
        const size_t headers_size = names_end - names_start;

        for (size_t i = 0; i < headers_size; ++i)
        {
            const auto name = names_data.getDataAt(names_start + i);
            const auto value = values_data.getDataAt(values_start + i);
            builder.header(cppkafka::MessageBuilder::HeaderType{
                std::string(name.data(), name.size()),
                cppkafka::Buffer(value.data(), value.size())});
        }
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

void KafkaProducer::cancel() noexcept
{
    /* no op */
}

}
