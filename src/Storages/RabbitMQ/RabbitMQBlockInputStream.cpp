#include <Formats/FormatFactory.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Storages/RabbitMQ/RabbitMQBlockInputStream.h>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

RabbitMQBlockInputStream::RabbitMQBlockInputStream(
    StorageRabbitMQ & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const Context & context_,
    const Names & columns)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , context(context_)
        , column_names(columns)
        , non_virtual_header(metadata_snapshot->getSampleBlockNonMaterialized())
        , virtual_header(metadata_snapshot->getSampleBlockForColumns({"_exchange"}, storage.getVirtuals(), storage.getStorageID()))
{
}


RabbitMQBlockInputStream::~RabbitMQBlockInputStream()
{
    if (!claimed)
        return;

    storage.pushReadBuffer(buffer);
}


Block RabbitMQBlockInputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());
}


void RabbitMQBlockInputStream::readPrefixImpl()
{
    auto timeout = std::chrono::milliseconds(context.getSettingsRef().rabbitmq_max_wait_ms.totalMilliseconds());

    buffer = storage.popReadBuffer(timeout);
    claimed = !!buffer;

    if (!buffer || finished)
        return;

    buffer->checkSubscription();
}


Block RabbitMQBlockInputStream::readImpl()
{
    if (!buffer || finished)
        return Block();

    finished = true;

    MutableColumns result_columns = non_virtual_header.cloneEmptyColumns();
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto input_format = FormatFactory::instance().getInputFormat(
            storage.getFormatName(), *buffer, non_virtual_header, context, 1);

    InputPort port(input_format->getPort().getHeader(), input_format.get());
    connect(input_format->getPort(), port);
    port.setNeeded();

    auto read_rabbitmq_message = [&]
    {
        size_t new_rows = 0;

        while (true)
        {
            auto status = input_format->prepare();

            switch (status)
            {
                case IProcessor::Status::Ready:
                    input_format->work();
                    break;

                case IProcessor::Status::Finished:
                    input_format->resetParser();
                    return new_rows;

                case IProcessor::Status::PortFull:
                {
                    auto chunk = port.pull();

                    auto chunk_rows = chunk.getNumRows();
                    new_rows += chunk_rows;

                    auto columns = chunk.detachColumns();

                    for (size_t i = 0, s = columns.size(); i < s; ++i)
                    {
                        result_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
                    }
                    break;
                }
                case IProcessor::Status::NeedData:
                case IProcessor::Status::Async:
                case IProcessor::Status::Wait:
                case IProcessor::Status::ExpandPipeline:
                    throw Exception("Source processor returned status " + IProcessor::statusToName(status), ErrorCodes::LOGICAL_ERROR);
            }
        }
    };

    size_t total_rows = 0;

    while (true)
    {
        if (buffer->eof())
            break;

        auto new_rows = read_rabbitmq_message();

        auto exchange_name = buffer->getExchange();

        for (size_t i = 0; i < new_rows; ++i)
        {
            virtual_columns[0]->insert(exchange_name);
        }

        total_rows = total_rows + new_rows;
        buffer->allowNext();

        if (!new_rows || !checkTimeLimit())
            break;
    }

    if (total_rows == 0)
        return Block();

    auto result_block  = non_virtual_header.cloneWithColumns(std::move(result_columns));
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
    {
        result_block.insert(column);
    }

    return result_block;
}

}
