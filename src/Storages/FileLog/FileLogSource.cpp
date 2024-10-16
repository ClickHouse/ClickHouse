#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/FileLog/FileLogConsumer.h>
#include <Storages/FileLog/FileLogSource.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{
static constexpr auto MAX_FAILED_POLL_ATTEMPTS = 10;

FileLogSource::FileLogSource(
    StorageFileLog & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    const Names & columns,
    size_t max_block_size_,
    size_t poll_time_out_,
    size_t stream_number_,
    size_t max_streams_number_,
    StreamingHandleErrorMode handle_error_mode_)
    : ISource(storage_snapshot_->getSampleBlockForColumns(columns))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , poll_time_out(poll_time_out_)
    , stream_number(stream_number_)
    , max_streams_number(max_streams_number_)
    , handle_error_mode(handle_error_mode_)
    , non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized())
    , virtual_header(storage_snapshot->virtual_columns->getSampleBlock())
{
    consumer = std::make_unique<FileLogConsumer>(storage, max_block_size, poll_time_out, context, stream_number_, max_streams_number_);

    const auto & file_infos = storage.getFileInfos();

    size_t files_per_stream = file_infos.file_names.size() / max_streams_number;
    start = stream_number * files_per_stream;
    end = stream_number == max_streams_number - 1 ? file_infos.file_names.size() : (stream_number + 1) * files_per_stream;

    storage.increaseStreams();
}

FileLogSource::~FileLogSource()
{
    try
    {
        if (!finished)
            onFinish();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void FileLogSource::onFinish()
{
    storage.closeFilesAndStoreMeta(start, end);
    storage.reduceStreams();
    finished = true;
}

Chunk FileLogSource::generate()
{
    /// Store metas of last written chunk into disk
    storage.storeMetas(start, end);

    if (!consumer || consumer->noRecords())
    {
        /// There is no onFinish for ISource, we call it
        /// when no records return to close files
        onFinish();
        return {};
    }

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        storage.getFormatName(), empty_buf, non_virtual_header, context, max_block_size, std::nullopt, 1);

    std::optional<String> exception_message;
    size_t total_rows = 0;

    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e)
    {
        if (handle_error_mode == StreamingHandleErrorMode::STREAM)
        {
            exception_message = e.message();
            for (size_t i = 0; i < result_columns.size(); ++i)
            {
                // We could already push some rows to result_columns before exception, we need to fix it.
                result_columns[i]->rollback(*checkpoints[i]);

                // All data columns will get default value in case of error.
                result_columns[i]->insertDefault();
            }

            return 1;
        }

        throw std::move(e);
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, on_error);

    size_t failed_poll_attempts = 0;

    Stopwatch watch;
    while (true)
    {
        exception_message.reset();
        size_t new_rows = 0;
        if (auto buf = consumer->consume())
            new_rows = executor.execute(*buf);

        if (new_rows)
        {
            auto file_name = consumer->getFileName();
            auto offset = consumer->getOffset();
            for (size_t i = 0; i < new_rows; ++i)
            {
                virtual_columns[0]->insert(file_name);
                virtual_columns[1]->insert(offset);
                if (handle_error_mode == StreamingHandleErrorMode::STREAM)
                {
                    if (exception_message)
                    {
                        const auto & current_record = consumer->getCurrentRecord();
                        virtual_columns[2]->insertData(current_record.data(), current_record.size());
                        virtual_columns[3]->insertData(exception_message->data(), exception_message->size());
                    }
                    else
                    {
                        virtual_columns[2]->insertDefault();
                        virtual_columns[3]->insertDefault();
                    }
                }
            }
            total_rows = total_rows + new_rows;
        }
        else /// poll succeed, but parse failed
        {
            ++failed_poll_attempts;
        }

        if (!consumer->hasMorePolledRecords()
            && ((total_rows >= max_block_size) || watch.elapsedMilliseconds() > poll_time_out
                || failed_poll_attempts >= MAX_FAILED_POLL_ATTEMPTS))
        {
            break;
        }
    }

    if (total_rows == 0)
    {
        onFinish();
        return {};
    }

    auto result_block = non_virtual_header.cloneWithColumns(executor.getResultColumns());
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    auto converting_dag = ActionsDAG::makeConvertingActions(
        result_block.cloneEmpty().getColumnsWithTypeAndName(),
        getPort().getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    converting_actions->execute(result_block);

    return Chunk(result_block.getColumns(), result_block.rows());
}

}
