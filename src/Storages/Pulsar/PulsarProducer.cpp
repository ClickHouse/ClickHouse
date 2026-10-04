#include <Storages/Pulsar/PulsarProducer.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Common/Logger.h>
#include <Common/assert_cast.h>

#include <pulsar/MessageBuilder.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ABORTED;
extern const int CANNOT_CONNECT_PULSAR;
}

PulsarProducer::PulsarProducer(
    ProducerPtr producer_, const std::string & topic_, std::atomic<bool> & shutdown_called_, const Block & header)
    : IMessageProducer(getLogger("PulsarProducer")), producer(producer_), topic(topic_), shutdown_called(shutdown_called_)
{
    if (header.has("_ordering_key"))
    {
        auto column_index = header.getPositionByName("_ordering_key");
        const auto & column_info = header.getByPosition(column_index);
        if (isString(column_info.type))
            key_column_index = column_index;
    }
}

void PulsarProducer::produce(const String & message, size_t /* rows_in_message */, const Columns & columns, size_t last_row)
{
    pulsar::MessageBuilder builder;
    builder.setContent(message);

    // Note: if it will be few rows per message - it will take the value from last row of block
    if (key_column_index)
    {
        const auto & key_column = assert_cast<const ColumnString &>(*columns[key_column_index.value()]);
        const auto key_data = key_column.getDataAt(last_row);
        builder.setOrderingKey(std::string(key_data));
    }

    auto final_message = builder.build();

    if (shutdown_called.load())
        throw Exception(ErrorCodes::ABORTED, "Cannot send message to Pulsar: table is shut down");

    /// The producer is configured with `setBlockIfQueueFull(true)`, so transient backpressure is
    /// handled by blocking inside `send` (bounded by the send timeout). Every non-OK result is
    /// therefore a real error and must fail the insert instead of being retried indefinitely.
    auto result = producer->send(final_message);
    if (result != pulsar::ResultOk)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_PULSAR,
            "Failed to send message to Pulsar topic {}: {}",
            topic,
            pulsar::strResult(result));
}

void PulsarProducer::finish()
{
    auto result = producer->flush();
    if (result != pulsar::ResultOk)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_PULSAR,
            "Failed to flush messages to Pulsar topic {}: {}",
            topic,
            pulsar::strResult(result));
}

void PulsarProducer::cancel() noexcept
{
    /// Closing the producer aborts a `send` blocked on a full queue and fails all pending sends,
    /// so a cancelled `INSERT` does not hang on a stuck broker.
    producer->close();
}

}
