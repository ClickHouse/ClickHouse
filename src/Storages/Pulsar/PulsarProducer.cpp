#include <Storages/Pulsar/PulsarProducer.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>


#include <pulsar/MessageBuilder.h>

namespace DB
{

PulsarProducer::PulsarProducer(
    ProducerPtr producer_,
    const std::string & topic_,
    std::atomic<bool> & shutdown_called_,
    const Block & header)
    : IMessageProducer(getLogger("PulsarProducer"))
    , producer(producer_)
    , topic(topic_)
    , shutdown_called(shutdown_called_)
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
        builder.setOrderingKey(key_data.toString());
    }

    auto final_message = builder.build();

    while (!shutdown_called.load())
    {
        auto result = producer->send(final_message);
        if (result != pulsar::ResultOk)
            continue;
        break;
    }
}

void PulsarProducer::finish()
{
    producer->flush();
}

}
