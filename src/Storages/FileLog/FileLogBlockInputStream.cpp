#include <Storages/FileLog/FileLogBlockInputStream.h>

#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Storages/FileLog/ReadBufferFromFileLog.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FileLogBlockInputStream::FileLogBlockInputStream(
    StorageFileLog & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const std::shared_ptr<Context> & context_,
    const Names & columns,
    size_t max_block_size_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , non_virtual_header(metadata_snapshot->getSampleBlockNonMaterialized())
    , virtual_header(
          metadata_snapshot->getSampleBlockForColumns(storage.getVirtualColumnNames(), storage.getVirtuals(), storage.getStorageID()))
{
}

Block FileLogBlockInputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());
}

void FileLogBlockInputStream::readPrefixImpl()
{
    buffer = storage.getBuffer();

    if (!buffer)
        return;

    buffer->open();
}

Block FileLogBlockInputStream::readImpl()
{
    if (!buffer)
        return Block();

    MutableColumns result_columns  = non_virtual_header.cloneEmptyColumns();

    auto input_format = FormatFactory::instance().getInputFormat(
        storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size);

    InputPort port(input_format->getPort().getHeader(), input_format.get());
    connect(input_format->getPort(), port);
    port.setNeeded();

    std::optional<std::string> exception_message;
    auto read_file_log = [&] {
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
        size_t new_rows = 0;
        exception_message.reset();
        if (buffer->poll())
        {
            try
            {
                new_rows = read_file_log();
            }
            catch (Exception &)
            {
                throw;
            }
        }
        if (new_rows)
        {
            total_rows = total_rows + new_rows;
        }

        if (!buffer->hasMorePolledRecords() && (total_rows >= max_block_size || !checkTimeLimit()))
        {
            break;
        }
    }

    if (total_rows == 0)
        return Block();

    auto result_block  = non_virtual_header.cloneWithColumns(std::move(result_columns));

    return ConvertingBlockInputStream(
               std::make_shared<OneBlockInputStream>(result_block),
               getHeader(),
               ConvertingBlockInputStream::MatchColumnsMode::Name)
        .read();
}

void FileLogBlockInputStream::readSuffixImpl()
{
    if (buffer)
        buffer->close();
}

}
