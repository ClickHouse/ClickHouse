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
    , virtual_header(storage_snapshot->getSampleBlockForColumns(storage.getVirtuals().getNames()))
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

    std::vector<String> exception_messages;
    size_t total_rows = 0;

    size_t input_format_allow_errors_num = context->getSettingsRef().input_format_allow_errors_num;
    size_t num_rows_with_errors = 0;
    auto on_error = [&](std::exception_ptr e)
    {
        if (handle_error_mode != StreamingHandleErrorMode::STREAM)
        {
            if (input_format_allow_errors_num >= ++num_rows_with_errors)
                return;

            std::rethrow_exception(e);
        }
        exception_messages.emplace_back(getExceptionMessage(e, false));
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, on_error);

    size_t failed_poll_attempts = 0;

    Stopwatch watch;
    while (true)
    {
        exception_messages.clear();
        size_t new_rows = 0;
        if (auto buf = consumer->consume())
            new_rows = executor.execute(*buf);

        if (new_rows || !exception_messages.empty())
        {
            auto file_name = consumer->getFileName();
            auto offset = consumer->getOffset();

            size_t new_rows_with_errors = new_rows + exception_messages.size();
            virtual_columns[0]->insertMany(file_name, new_rows_with_errors);
            virtual_columns[1]->insertMany(offset, new_rows_with_errors);

            if (handle_error_mode == StreamingHandleErrorMode::STREAM)
            {
                virtual_columns[2]->insertManyDefaults(new_rows);
                virtual_columns[3]->insertManyDefaults(new_rows);

                const auto & current_record = consumer->getCurrentRecord();
                virtual_columns[2]->insertMany(Field(current_record.data(), current_record.size()), exception_messages.size());

                for (const auto & exception_message : exception_messages)
                    virtual_columns[3]->insertData(exception_message.data(), exception_message.size());
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

    size_t result_block_rows = result_block.rows();
    size_t virtual_block_rows = virtual_block.rows();
    size_t errors = virtual_block_rows - result_block_rows;
    if (errors)
    {
        auto result_columns = result_block.mutateColumns();
        for (auto & column : result_columns)
            column->insertManyDefaults(errors);
        result_block.setColumns(std::move(result_columns));
    }

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
