#include <Storages/Pulsar/PulsarProducer.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/assert_cast.h>

#include <optional>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
extern const int ABORTED;
}

PulsarProducer::PulsarProducer(
    ProducerPtr producer_, const std::string & topic_, std::atomic<bool> & shutdown_called_, const Block & header)
    : IMessageProducer(getLogger("PulsarProducer")), producer(std::move(producer_)), topic(topic_), shutdown_called(shutdown_called_)
{
    if (header.has("_ordering_key"))
    {
        auto column_index = header.getPositionByName("_ordering_key");
        const auto & column_info = header.getByPosition(column_index);
        if (isString(column_info.type))
            key_column_index = column_index;
    }
}

PulsarProducer::~PulsarProducer()
{
    /// Unregister the publisher on the broker: each `INSERT` creates its own producer, so one
    /// that failed before `finish` must not stay registered. Closing a closed producer does nothing.
    producer->close();
}

void PulsarProducer::produce(const String & message, size_t /* rows_in_message */, const Columns & columns, size_t last_row)
{
    /// If there are several rows per message, the value of the last row of the message is used.
    std::optional<std::string_view> ordering_key;
    if (key_column_index)
    {
        const auto & key_column = assert_cast<const ColumnString &>(*columns[key_column_index.value()]);
        ordering_key = key_column.getDataAt(last_row);
    }

    if (shutdown_called.load())
        throw Exception(ErrorCodes::ABORTED, "Cannot send message to Pulsar: table is shut down");

    /// `send` waits for the receipt of the broker, reconnecting if needed, within the send timeout.
    /// An exception is therefore a real error and fails the insert.
    try
    {
        producer->send(message, ordering_key);
    }
    catch (Exception & e)
    {
        e.addMessage("While sending a message to the Pulsar topic {}", topic);
        throw;
    }
}

void PulsarProducer::finish()
{
    /// Nothing is pending: every `send` has already waited for the receipt of the broker.
    /// Unregister the publisher on the broker, because each `INSERT` creates its own producer.
    producer->close();
}

void PulsarProducer::cancel() noexcept
{
    /// Makes the `send` in progress (possibly on another thread) and all later ones throw,
    /// so a cancelled `INSERT` does not hang on a stuck broker. The destructor closes the producer.
    producer->interrupt();
}

}
