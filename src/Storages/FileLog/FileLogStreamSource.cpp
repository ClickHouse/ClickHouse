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
#include <Common/Hex.h>
#include <Common/StringUtils.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <sys/stat.h>

#include <chrono>
#include <thread>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_STAT;
extern const int CANNOT_OPEN_FILE;
}

namespace
{

ContextPtr makeFormatContext(const ContextPtr & context)
{
    auto format_context = Context::createCopy(context);
    format_context->setSetting("input_format_csv_detect_header", false);
    format_context->setSetting("input_format_tsv_detect_header", false);
    format_context->setSetting("input_format_custom_detect_header", false);
    return format_context;
}

/// The cursor tree splits its keys by dots, so a file name is a single key, where `.` is written as `%2E` and `%` as `%25`.
String encodeFileName(const String & file_name)
{
    String result;
    result.reserve(file_name.size());
    for (char c : file_name)
    {
        if (c == '.')
            result += "%2E";
        else if (c == '%')
            result += "%25";
        else
            result += c;
    }
    return result;
}

String decodeFileName(const String & key)
{
    String result;
    result.reserve(key.size());
    for (size_t i = 0; i < key.size(); ++i)
    {
        if (key[i] != '%')
        {
            result += key[i];
            continue;
        }
        if (i + 2 >= key.size() || !isHexDigit(key[i + 1]) || !isHexDigit(key[i + 2]))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Invalid escape sequence in the file name '{}' in the cursor: a dot is written as %2E and a percent sign as %25", key);
        result += static_cast<char>(unhex2(&key[i + 1]));
        i += 2;
    }
    return result;
}

/// A record starts at the beginning of a file or after a newline.
bool isRecordStart(const String & file_path, UInt64 offset)
{
    if (offset == 0)
        return true;

    std::ifstream reader(file_path, std::ios::binary);
    reader.seekg(static_cast<std::streamoff>(offset - 1));
    char c = 0;
    reader.get(c);
    if (!reader.good())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot read file {} at offset {}", file_path, offset - 1);
    return c == '\n';
}

/// An entry of the cursor of the query.
struct CursorEntry
{
    /// The offset of the last read record.
    Int64 offset = 0;
    /// The cursor that a bounded read reports for an incremental refresh has the identity of the file (`inode`) and
    /// the position after the last read record (`end`): {'<file name>': {'offset': ..., 'inode': ..., 'end': ...}}.
    /// A file that does not match them was replaced or truncated since the cursor was taken and is read from the beginning.
    bool structured = false;
    std::optional<UInt64> inode;
    std::optional<UInt64> end;
};

std::map<String, CursorEntry> parseCursor(const CursorTreeNodePtr & cursor)
{
    std::map<String, CursorEntry> result;
    if (!cursor)
        return result;

    for (const auto & [key, value] : *cursor)
    {
        const String file_name = decodeFileName(key);
        CursorEntry entry;
        if (const auto * offset = std::get_if<Int64>(&value))
        {
            entry.offset = *offset;
        }
        else
        {
            entry.structured = true;
            std::optional<Int64> offset_field;
            for (const auto & [field, field_value] : *std::get<CursorTreeNodePtr>(value))
            {
                const auto * number = std::get_if<Int64>(&field_value);
                if (number && field == "offset")
                    offset_field = *number;
                else if (number && field == "inode")
                    entry.inode = static_cast<UInt64>(*number);
                else if (number && field == "end" && *number >= 0)
                    entry.end = static_cast<UInt64>(*number);
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unexpected key '{}' in the cursor for file '{}'. The cursor of a FileLog table is "
                        "{{'<file name>': <offset of the last read record>}}, where a dot in the file name is written as %2E",
                        field, file_name);
            }
            if (!offset_field)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The cursor for file '{}' has no offset", file_name);
            entry.offset = *offset_field;
        }
        result.emplace(file_name, entry);
    }
    return result;
}

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
    , format_context(makeFormatContext(context_))
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

    /// The current files by inode. A file with several names (hard links) is read once, under the first name.
    struct CurrentFile
    {
        String name;
        UInt64 size = 0;
    };
    std::map<UInt64, CurrentFile> current_files;
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
        current_files.try_emplace(static_cast<UInt64>(file_stat.st_ino), CurrentFile{file_name, static_cast<UInt64>(file_stat.st_size)});
    }

    /// A file that left the directory is closed after its remaining records are read.
    for (auto & [inode, state] : files)
        state.removed = !current_files.contains(inode);

    /// The cursor is read only at the start: a file that appears later is a new file.
    std::map<String, CursorEntry> cursor_entries;
    std::map<UInt64, const CursorEntry *> cursor_entries_by_inode;
    if (initial)
    {
        cursor_entries = parseCursor(stream_settings.cursor);
        for (const auto & [file_name, entry] : cursor_entries)
            if (entry.inode)
                cursor_entries_by_inode.emplace(*entry.inode, &entry);
    }

    for (const auto & [inode, current_file] : current_files)
    {
        const auto & [file_name, size] = current_file;

        if (auto it = files.find(inode); it != files.end())
        {
            auto & state = it->second;
            if (state.name != file_name)
            {
                LOG_DEBUG(log, "File {} was renamed to {}, continuing to read it", state.name, file_name);
                state.name = file_name;
            }
            if (size < state.position)
            {
                /// The file was truncated in place (e.g. by copytruncate): read it from the beginning.
                LOG_DEBUG(log, "File {} was truncated, reading it from the beginning", file_name);
                state.position = 0;
                state.last_offset = -1;
                state.skip_record_at_position = false;
            }
            continue;
        }

        /// A bounded stream reads only the files that exist at its start.
        if (bounded && !initial)
            continue;

        FileState state;
        state.name = file_name;
        const String file_path = storage.getFullDataPath(file_name);

        /// The entry of the cursor for the file: by name, or by inode for a file renamed since the cursor was taken
        /// (log rotation). An entry of the same name for another inode belongs to a file replaced by this one.
        const CursorEntry * entry = nullptr;
        if (auto it_entry = cursor_entries.find(file_name);
            it_entry != cursor_entries.end() && (!it_entry->second.inode || *it_entry->second.inode == inode))
            entry = &it_entry->second;
        else if (auto it_inode = cursor_entries_by_inode.find(inode); it_inode != cursor_entries_by_inode.end())
            entry = it_inode->second;
        else if (it_entry != cursor_entries.end())
            LOG_DEBUG(log, "File {} was replaced since the cursor, reading it from the beginning", file_name);

        if (entry && entry->structured)
        {
            /// The cursor of an incremental refresh: a file truncated since then is read from the beginning.
            const bool valid = entry->offset >= 0
                && (entry->end ? *entry->end > static_cast<UInt64>(entry->offset) && *entry->end <= size
                                     && isRecordStart(file_path, *entry->end)
                               : static_cast<UInt64>(entry->offset) < size && isRecordStart(file_path, static_cast<UInt64>(entry->offset)));
            if (!valid)
            {
                LOG_DEBUG(log, "File {} was truncated since the cursor, reading it from the beginning", file_name);
            }
            else if (entry->end)
            {
                state.last_offset = entry->offset;
                state.position = *entry->end;
            }
            else
            {
                state.last_offset = entry->offset;
                state.position = entry->offset;
                state.skip_record_at_position = true;
            }
        }
        else if (entry)
        {
            if (entry->offset < 0 || static_cast<UInt64>(entry->offset) >= size)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Offset {} in the cursor for file {} is outside the file of size {}", entry->offset, file_name, size);
            if (!isRecordStart(file_path, static_cast<UInt64>(entry->offset)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Offset {} in the cursor for file {} is not the beginning of a record", entry->offset, file_name);
            state.last_offset = entry->offset;
            state.position = entry->offset;
            state.skip_record_at_position = true;
        }

        if (bounded)
            state.bound = size;

        files.emplace(inode, std::move(state));
    }
}

void FileLogStreamSource::reportFinalCursor() const
{
    /// Only an incremental refresh of a materialized view has the holder of the cursor.
    auto streaming_cursor = context->getStreamingCursor();
    if (!streaming_cursor)
        return;

    auto cursor = std::make_shared<CursorTreeNode>();
    for (const auto & [inode, state] : files)
    {
        if (state.last_offset < 0)
            continue;
        auto & entry = cursor->getSubtreeOrCreate(encodeFileName(state.name));
        entry->setValue("offset", state.last_offset);
        entry->setValue("inode", static_cast<Int64>(inode));
        /// The record at the offset of the cursor was not read yet (it is incomplete): it is skipped by the next read.
        if (!state.skip_record_at_position)
            entry->setValue("end", static_cast<Int64>(state.position));
    }
    streaming_cursor->merge(cursor);
}

std::optional<Chunk> FileLogStreamSource::tryGenerate()
{
    /// An idle stream returns an empty chunk after a poll, so that the thread runs other processors in the meantime
    /// (e.g. the other tables of a `Merge` table) instead of waiting here.
    auto chunk = generate();
    if (!chunk && (stream_finished || isCancelled()))
        return std::nullopt;
    return chunk;
}

Chunk FileLogStreamSource::generate()
{
    if (stream_finished)
        return {};

    const bool bounded = !stream_settings.subscribe_for_updates;

    if (!started)
    {
        updateFiles(/* initial = */ true);
        started = true;
        poll_watch.restart();
    }
    else if (!bounded && poll_watch.elapsedMilliseconds() >= poll_timeout_ms)
    {
        /// Check the directory every `poll_timeout_ms`, even while there is a backlog of records to read.
        updateFiles(/* initial = */ false);
        poll_watch.restart();
    }

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    EmptyReadBuffer empty_buf;
    auto parser_shared_resources = FormatParserSharedResources::singleThreaded(format_context->getSettingsRef());
    auto create_format = [&]
    {
        return FormatFactory::instance().getInput(
            storage.getFormatName(), empty_buf, non_virtual_header, format_context, max_block_size, std::nullopt, parser_shared_resources);
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

    /// Start from the file after the one that filled the previous chunk, so that a file with a backlog does not starve the others.
    std::vector<UInt64> order;
    order.reserve(files.size());
    const auto start_it = files.lower_bound(next_inode);
    for (auto it = start_it; it != files.end(); ++it)
        order.push_back(it->first);
    for (auto it = files.begin(); it != start_it; ++it)
        order.push_back(it->first);

    std::vector<UInt64> drained_removed_files;
    for (const UInt64 inode : order)
    {
        if (isCancelled())
            break;

        auto & state = files.at(inode);
        const String & file_name = state.name;
        const String file_path = storage.getFullDataPath(file_name);
        if (!state.reader)
        {
            state.reader = std::make_unique<std::ifstream>(file_path, std::ios::binary);
            if (!state.reader->is_open())
            {
                /// The file was removed. Keep its state for the cursor, unless the stream noticed that it left the directory.
                state.reader.reset();
                if (state.removed)
                    drained_removed_files.push_back(inode);
                continue;
            }
        }

        auto & reader = *state.reader;
        reader.clear();
        reader.seekg(static_cast<std::streamoff>(state.position));
        if (!reader.good())
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot seek to offset {} in file {}", state.position, file_path);

        bool reached_end = false;
        while (total_rows < max_block_size)
        {
            const UInt64 offset = state.position;
            if (state.bound && offset >= *state.bound)
            {
                reached_end = true;
                break;
            }

            std::getline(reader, line);

            /// A last line without a newline may still be being written: leave it for a later read.
            if (reader.eof() || reader.fail())
            {
                reader.clear();
                reached_end = true;
                break;
            }

            /// A bounded stream reads only the records that are complete within the size of the file at its start.
            const UInt64 next_position = offset + line.size() + 1;
            if (state.bound && next_position > *state.bound)
            {
                reached_end = true;
                break;
            }

            state.position = next_position;

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

        if (reached_end && state.removed)
            drained_removed_files.push_back(inode);

        if (total_rows >= max_block_size)
        {
            next_inode = inode + 1;
            break;
        }
    }

    /// Close the files that left the directory and have no complete records left: a deleted file must not be held open.
    for (const UInt64 inode : drained_removed_files)
    {
        LOG_DEBUG(log, "File {} left the directory, closing it", files.at(inode).name);
        files.erase(inode);
    }

    if (!total_rows && !isCancelled())
    {
        if (bounded)
        {
            /// A pass without new records: everything up to the sizes at the start is read.
            reportFinalCursor();
            stream_finished = true;
        }
        else
        {
            /// Wait for new data until the next check of the directory, checking for cancellation of the query in the meantime.
            while (!isCancelled() && poll_watch.elapsedMilliseconds() < poll_timeout_ms)
                std::this_thread::sleep_for(std::chrono::milliseconds(std::min<UInt64>(100, poll_timeout_ms - poll_watch.elapsedMilliseconds())));
        }
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
