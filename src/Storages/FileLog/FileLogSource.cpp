#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Storages/FileLog/FileLogSource.h>
#include <Storages/FileLog/ReadBufferFromFileLog.h>
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FileLogSource::FileLogSource(
    StorageFileLog & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ContextPtr & context_,
    const Names & columns,
    size_t max_block_size_,
    size_t poll_time_out_,
    size_t stream_number_,
    size_t max_streams_number_)
    : SourceWithProgress(metadata_snapshot_->getSampleBlockForColumns(columns, storage_.getVirtuals(), storage_.getStorageID()))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , poll_time_out(poll_time_out_)
    , non_virtual_header(metadata_snapshot_->getSampleBlockNonMaterialized())
    , column_names_and_types(metadata_snapshot_->getColumns().getByNames(ColumnsDescription::All, columns, true))
{
    buffer = std::make_unique<ReadBufferFromFileLog>(storage, max_block_size, poll_time_out, context, stream_number_, max_streams_number_);
}

Chunk FileLogSource::generate()
{
    if (!buffer)
        return {};

    MutableColumns read_columns = non_virtual_header.cloneEmptyColumns();

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
                        read_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
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
        Stopwatch watch;
        size_t new_rows = 0;
        if (buffer->poll())
        {
            try
            {
                new_rows = read_file_log();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
        if (new_rows)
        {
            total_rows = total_rows + new_rows;
        }

        if (!buffer->hasMorePolledRecords() && ((total_rows >= max_block_size) || watch.elapsedMilliseconds() > poll_time_out))
        {
            break;
        }
    }

    if (total_rows == 0)
        return {};

    Columns result_columns;
    result_columns.reserve(column_names_and_types.size());

    for (const auto & elem : column_names_and_types)
    {
        auto index = non_virtual_header.getPositionByName(elem.getNameInStorage());
        result_columns.emplace_back(std::move(read_columns[index]));
    }

    return Chunk(std::move(result_columns), total_rows);
}

}
