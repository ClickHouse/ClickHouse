#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Storages/FileLog/FileLogSource.h>
#include <Storages/FileLog/ReadBufferFromFileLog.h>
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>

namespace DB
{
static constexpr auto MAX_FAILED_POLL_ATTEMPTS = 10;

FileLogSource::FileLogSource(
    StorageFileLog & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ContextPtr & context_,
    size_t max_block_size_,
    size_t poll_time_out_,
    size_t stream_number_,
    size_t max_streams_number_)
    : SourceWithProgress(metadata_snapshot_->getSampleBlockWithVirtuals(storage_.getVirtuals()))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , max_block_size(max_block_size_)
    , poll_time_out(poll_time_out_)
    , non_virtual_header(metadata_snapshot_->getSampleBlockNonMaterialized())
    , virtual_header(
          metadata_snapshot->getSampleBlockForColumns(storage.getVirtualColumnNames(), storage.getVirtuals(), storage.getStorageID()))
{
    buffer = std::make_unique<ReadBufferFromFileLog>(storage, max_block_size, poll_time_out, context, stream_number_, max_streams_number_);
}

Chunk FileLogSource::generate()
{
    if (!buffer || buffer->noRecords())
        return {};

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto input_format
        = FormatFactory::instance().getInputFormat(storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size);

    StreamingFormatExecutor executor(non_virtual_header, input_format);

    size_t total_rows = 0;
    size_t failed_poll_attempts = 0;

    Stopwatch watch;
    while (true)
    {
        size_t new_rows = 0;
        if (buffer->poll())
            new_rows = executor.execute();

        if (new_rows)
        {
            auto file_name = buffer->getFileName();
            auto offset = buffer->getOffset();
            for (size_t i = 0; i < new_rows; ++i)
            {
                virtual_columns[0]->insert(file_name);
                virtual_columns[1]->insert(offset);
            }
            total_rows = total_rows + new_rows;
        }
        else /// poll succeed, but parse failed
        {
            ++failed_poll_attempts;
        }

        if (!buffer->hasMorePolledRecords()
            && ((total_rows >= max_block_size) || watch.elapsedMilliseconds() > poll_time_out
                || failed_poll_attempts >= MAX_FAILED_POLL_ATTEMPTS))
        {
            break;
        }
    }

    if (total_rows == 0)
        return {};

    auto result_columns = executor.getResultColumns();

    for (auto & column : virtual_columns)
    {
        result_columns.emplace_back(std::move(column));
    }

    return Chunk(std::move(result_columns), total_rows);
}

}
