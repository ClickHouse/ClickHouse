#include <Storages/FileLog/FileLogStreamSource.h>

#include <Columns/IColumn.h>
#include <Core/Streaming/CursorTree.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/FileLog/StorageFileLog.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/ErrnoException.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <sys/stat.h>

#include <chrono>
#include <thread>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_STAT;
extern const int CANNOT_OPEN_FILE;
extern const int INVALID_CURSOR_LOOKUP;
}

FileLogStreamSource::FileLogStreamSource(
    StorageFileLog & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    const Names & columns,
    const StreamSettings & stream_settings_,
    size_t max_block_size_,
    size_t poll_timeout_ms_)
    : ISource(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(columns)))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , stream_settings(stream_settings_)
    , max_block_size(std::max<size_t>(1, max_block_size_))
    , poll_timeout_ms(poll_timeout_ms_)
    , handle_error_mode(storage.getStreamingHandleErrorMode())
    , log(storage.getLog())
    , non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized())
    , virtual_header(storage_snapshot->metadata->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader))
{
}

FileLogStreamSource::~FileLogStreamSource() = default;

void FileLogStreamSource::updateFiles(bool initial)
{
    const bool bounded = !stream_settings.subscribe_for_updates;

    for (const auto & file_name : storage.listStreamFiles())
    {
        const String file_path = storage.getFullDataPath(file_name);
        struct stat file_stat{};
        if (stat(file_path.c_str(), &file_stat) != 0)
        {
            /// The file was removed after listing the directory.
            if (errno == ENOENT)
                continue;
            throw ErrnoException(ErrorCodes::CANNOT_STAT, "Cannot stat file {}", file_path);
        }
        const auto inode = static_cast<UInt64>(file_stat.st_ino);
        const auto size = static_cast<UInt64>(file_stat.st_size);

        auto it = files.find(file_name);
        if (it == files.end())
        {
            /// A bounded stream reads only the files that exist at its start.
            if (bounded && !initial)
                continue;

            FileState state;
            state.inode = inode;

            if (stream_settings.cursor && stream_settings.cursor->hasSubtree(file_name))
                throw Exception(ErrorCodes::INVALID_CURSOR_LOOKUP,
                    "The cursor of a FileLog table must map a file name to an offset, but it has a subtree for file {}", file_name);

            /// The cursor is read only at the start: a file that appears later is a new file.
            if (initial && stream_settings.cursor && stream_settings.cursor->hasValue(file_name))
            {
                const Int64 offset = stream_settings.cursor->getValue(file_name);
                if (offset < 0 || static_cast<UInt64>(offset) >= size)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Offset {} in the cursor for file {} is outside the file of size {}", offset, file_name, size);
                state.last_offset = offset;
                state.position = offset;
                state.skip_record_at_position = true;
            }

            if (bounded)
                state.end = size;

            files.emplace(file_name, std::move(state));
        }
        else if (it->second.inode != inode)
        {
            /// The file was replaced by another one with the same name (e.g. by log rotation): read the new file.
            LOG_DEBUG(log, "File {} was replaced, reading it from the beginning", file_name);
            FileState state;
            state.inode = inode;
            it->second = std::move(state);
        }
    }
}

void FileLogStreamSource::reportFinalCursor() const
{
    auto streaming_cursor = context->getStreamingCursor();
    if (!streaming_cursor)
        return;

    /// Keep the entries of the cursor of the query for the files that do not exist anymore.
    auto cursor = stream_settings.cursor ? stream_settings.cursor->clone() : std::make_shared<CursorTreeNode>();
    for (const auto & [file_name, state] : files)
        if (state.last_offset >= 0)
            cursor->setValue(file_name, state.last_offset);

    streaming_cursor->merge(cursor);
}

Chunk FileLogStreamSource::generate()
{
    if (stream_finished)
        return {};

    if (!started)
    {
        updateFiles(/* initial = */ true);
        started = true;
    }

    const bool bounded = !stream_settings.subscribe_for_updates;

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    EmptyReadBuffer empty_buf;
    auto parser_shared_resources = FormatParserSharedResources::singleThreaded(context->getSettingsRef());
    auto create_format = [&]
    {
        return FormatFactory::instance().getInput(
            storage.getFormatName(), empty_buf, non_virtual_header, context, max_block_size, std::nullopt, parser_shared_resources);
    };

    std::optional<String> exception_message;
    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e) -> size_t
    {
        if (handle_error_mode != StreamingHandleErrorMode::STREAM)
            throw std::move(e);

        exception_message = e.message();
        for (size_t i = 0; i < result_columns.size(); ++i)
        {
            result_columns[i]->rollback(*checkpoints[i]);
            result_columns[i]->insertDefault();
        }
        return 1;
    };

    StreamingFormatExecutor executor(non_virtual_header, std::move(create_format), std::move(on_error));

    const Field table_name = storage.getStorageID().getTableName();
    size_t total_rows = 0;
    String line;

    while (!isCancelled())
    {
        for (auto & [file_name, state] : files)
        {
            if (total_rows >= max_block_size)
                break;

            const String file_path = storage.getFullDataPath(file_name);
            if (!state.reader)
            {
                state.reader = std::make_unique<std::ifstream>(file_path, std::ios::binary);
                if (!state.reader->is_open())
                {
                    /// The file was removed. Keep its state for the cursor.
                    state.reader.reset();
                    continue;
                }
            }

            auto & reader = *state.reader;
            reader.clear();
            reader.seekg(static_cast<std::streamoff>(state.position));
            if (!reader.good())
                throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot seek to offset {} in file {}", state.position, file_path);

            while (total_rows < max_block_size)
            {
                const UInt64 offset = state.position;
                if (state.end && offset >= *state.end)
                    break;

                std::getline(reader, line);

                /// A last line without a newline may still be being written: leave it for a later read.
                if (reader.eof() || reader.fail())
                {
                    reader.clear();
                    break;
                }

                state.position = offset + line.size() + 1;

                if (state.skip_record_at_position)
                {
                    /// It is the record at the offset of the cursor, which was already read.
                    state.skip_record_at_position = false;
                    continue;
                }

                exception_message.reset();
                ReadBufferFromString buf(line);
                const size_t new_rows = executor.execute(buf);
                state.last_offset = static_cast<Int64>(offset);

                for (size_t i = 0; i < new_rows; ++i)
                {
                    for (size_t column_index = 0; column_index < virtual_columns.size(); ++column_index)
                    {
                        const auto & name = virtual_header.getByPosition(column_index).name;
                        auto & column = virtual_columns[column_index];
                        if (name == "_filename")
                            column->insert(file_name);
                        else if (name == "_offset")
                            column->insert(offset);
                        else if (name == "_table")
                            column->insert(table_name);
                        else if (name == "_raw_record" && exception_message)
                            column->insertData(line.data(), line.size());
                        else if (name == "_error" && exception_message)
                            column->insertData(exception_message->data(), exception_message->size());
                        else
                            column->insertDefault();
                    }
                }

                total_rows += new_rows;
            }
        }

        if (total_rows)
            break;

        /// A pass without new records: everything up to the sizes at the start is read.
        if (bounded)
        {
            reportFinalCursor();
            stream_finished = true;
            break;
        }

        /// Wait for new data, checking for cancellation of the query in the meantime.
        Stopwatch watch;
        while (!isCancelled() && watch.elapsedMilliseconds() < poll_timeout_ms)
            std::this_thread::sleep_for(std::chrono::milliseconds(std::min<UInt64>(100, poll_timeout_ms - watch.elapsedMilliseconds())));

        updateFiles(/* initial = */ false);
    }

    if (!total_rows)
        return {};

    auto result_block = non_virtual_header.cloneWithColumns(executor.getResultColumns());
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));
    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    auto converting_dag = ActionsDAG::makeConvertingActions(
        result_block.cloneEmpty().getColumnsWithTypeAndName(),
        getPort().getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);
    ExpressionActions(std::move(converting_dag)).execute(result_block);

    return Chunk(result_block.getColumns(), result_block.rows());
}

}
