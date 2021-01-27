#include "PostgreSQLReplicaBlockInputStream.h"

#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

PostgreSQLReplicaBlockInputStream::PostgreSQLReplicaBlockInputStream(
    StoragePostgreSQLReplica & storage_,
    ConsumerBufferPtr buffer_,
    const StorageMetadataPtr & metadata_snapshot_,
    std::shared_ptr<Context> context_,
    const Names & columns,
    size_t max_block_size_)
    : storage(storage_)
    , buffer(buffer_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , non_virtual_header(metadata_snapshot->getSampleBlockNonMaterialized())
    , sample_block(non_virtual_header)
    , virtual_header(metadata_snapshot->getSampleBlockForColumns({}, storage.getVirtuals(), storage.getStorageID()))
{
    for (const auto & column : virtual_header)
        sample_block.insert(column);
}


PostgreSQLReplicaBlockInputStream::~PostgreSQLReplicaBlockInputStream()
{
}


void PostgreSQLReplicaBlockInputStream::readPrefixImpl()
{
}


Block PostgreSQLReplicaBlockInputStream::readImpl()
{
    if (!buffer || finished)
        return Block();

    finished = true;

    MutableColumns result_columns = non_virtual_header.cloneEmptyColumns();
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto input_format = FormatFactory::instance().getInputFormat(
            "Values", *buffer, non_virtual_header, *context, max_block_size);

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

        if (new_rows)
        {
            //auto timestamp = buffer->getTimestamp();
            //for (size_t i = 0; i < new_rows; ++i)
            //{
            //    virtual_columns[0]->insert(timestamp);
            //}

            total_rows = total_rows + new_rows;
        }

        buffer->allowNext();

        if (total_rows >= max_block_size || !checkTimeLimit())
            break;
    }

    if (total_rows == 0)
        return Block();

    auto result_block  = non_virtual_header.cloneWithColumns(std::move(result_columns));
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    return result_block;
}


void PostgreSQLReplicaBlockInputStream::readSuffixImpl()
{
}

}
