#include <Storages/RabbitMQ/RabbitMQBlockInputStream.h>

#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>

#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>

namespace DB
{

RabbitMQBlockInputStream::RabbitMQBlockInputStream(
        StorageRabbitMQ & storage_, const Context & context_, const Names & columns,
        size_t max_block_size_, Poco::Logger * log_, bool commit_in_suffix_)
        : storage(storage_)
        , context(context_)
        , column_names(columns)
        , max_block_size(max_block_size_)
        , log(log_)
        , commit_in_suffix(commit_in_suffix_)
        , non_virtual_header(storage.getSampleBlockNonMaterialized())
        , virtual_header(storage.getSampleBlockForColumns(
                {"_exchange", "_routingKey", "_deliveryTag"}) 
        )
{
    context.setSetting("input_format_skip_unknown_fields", 1u); // Always skip unknown fields regardless of the context
    context.setSetting("input_format_allow_errors_ratio", 0.);
    context.setSetting("input_format_allow_errors_num", storage.skipBroken());
}


RabbitMQBlockInputStream::~RabbitMQBlockInputStream()
{
    if (!claimed)
        return;

    storage.pushReadBuffer(buffer);
}

Block RabbitMQBlockInputStream::getHeader() const
{
    return storage.getSampleBlockForColumns(column_names);
}


void RabbitMQBlockInputStream::readPrefixImpl()
{
    LOG_DEBUG(log, "ReadPrefixImpl.\n");

    auto timeout = std::chrono::milliseconds(context.getSettingsRef().rabbitmq_max_wait_ms.totalMilliseconds());

    buffer = storage.popReadBuffer(timeout);
    claimed = !!buffer;

    if (!buffer)
        return;

    buffer->subscribe(storage.getRoutingKeys());
}

Block RabbitMQBlockInputStream::readImpl()
{
    LOG_DEBUG(log, "ReadImpl.\n");

    if (!buffer)
        return Block();

    LOG_DEBUG(log, "Starting writing data into table");

    MutableColumns result_columns  = non_virtual_header.cloneEmptyColumns();
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto input_format = FormatFactory::instance().getInputFormat(
            storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size);

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

        auto _exchange = buffer->getCurrentExchange();
        auto _routingKey = buffer->getCurrentRoutingKey();
        auto _deliveryTag = buffer->getCurrentDeliveryTag();

        for (size_t i = 0; i < new_rows; ++i)
        {
            virtual_columns[0]->insert(_exchange);
            virtual_columns[1]->insert(_routingKey);
            virtual_columns[2]->insert(_deliveryTag);
        }

        total_rows = total_rows + new_rows;

        buffer->allowNext();

        if (!new_rows || total_rows >= max_block_size || !checkTimeLimit())
            break;
    }

    if (total_rows == 0)
        return Block();

    auto result_block  = non_virtual_header.cloneWithColumns(std::move(result_columns));
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    return ConvertingBlockInputStream(
            context,
            std::make_shared<OneBlockInputStream>(result_block),
            getHeader(),
            ConvertingBlockInputStream::MatchColumnsMode::Name)
            .read();
}

void RabbitMQBlockInputStream::readSuffixImpl()
{
    LOG_DEBUG(log, "ReadSuffixImpl.");
    LOG_DEBUG(log, "Check for commit.");
    if (commit_in_suffix)
    {
        commit();
    }
}

void RabbitMQBlockInputStream::commit()
{
    LOG_DEBUG(log, "Commit.");
    if (!buffer)
        return;

    LOG_DEBUG(log, "Commit is called: starting processing.");
    /// startProcessing();
}

void RabbitMQBlockInputStream::commitNotSubscribed(const Names & routing_keys)
{
    LOG_DEBUG(log, "CommitNotSubscribed.");

    if (!buffer)
        return;

    LOG_DEBUG(log, "Starting commit for not subscribed.");
    buffer->commitNotSubscribed(routing_keys);
    /// startProcessing();
}

}
