#pragma once

#include <Core/Block.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Core/Streaming/Settings.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/ISource.h>
#include <Storages/StorageSnapshot.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>

#include <fstream>
#include <map>
#include <memory>
#include <optional>


namespace DB
{

class StorageFileLog;

/// Reads the files of a `FileLog` table for a streaming query (`SELECT ... FROM file_log STREAM`).
///
/// Unlike a direct `SELECT` from the table, it does not consume the records: it reads the files with its own readers
/// and never changes the read positions stored in the metadata of the table. It starts at the cursor of the query:
///     {'<file name>': <offset of the last read record>, ...}
/// The cursor splits its keys by dots, so a dot in a file name is written as `%2E` (and `%` as `%25`), e.g.
/// {'app%2Elog': 1024}. A file absent in the cursor is read from the beginning. The cursor of the read can be
/// reconstructed from the `_filename` and `_offset` virtual columns. Only complete records (ending with a newline) are read.
///
/// The files are identified by their inodes: a file renamed by log rotation keeps its read position, and a file with
/// several names (hard links) is read once. A file that leaves the directory is closed after its remaining complete
/// records are read.
///
/// An unbounded stream checks the directory for new files and new data every `poll_timeout_ms`.
/// A bounded stream (`STREAM BOUNDED`) reads the files up to their sizes at the start of the query.
class FileLogStreamSource final : public ISource
{
public:
    FileLogStreamSource(
        StorageFileLog & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Names & columns,
        const StreamSettings & stream_settings_,
        size_t max_block_size_,
        size_t poll_timeout_ms_);

    ~FileLogStreamSource() override;

    String getName() const override { return "FileLogStreamSource"; }

    Chunk generate() override;
    std::optional<Chunk> tryGenerate() override;

private:
    struct FileState
    {
        /// The current name of the file (a file may be renamed while it is read).
        String name;
        /// The offset of the last record read (or of the cursor), -1 if nothing was read.
        Int64 last_offset = -1;
        /// The position to read the next record from.
        UInt64 position = 0;
        /// The record at `last_offset` from the cursor still has to be skipped.
        bool skip_record_at_position = false;
        /// A bounded stream reads only the records that end within this size.
        std::optional<UInt64> bound;
        /// The file is not in the directory any more: it is closed after its remaining complete records are read.
        bool removed = false;
        std::unique_ptr<std::ifstream> reader;
    };

    void updateFiles(bool initial);
    void reportFinalCursor() const;

    StorageFileLog & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    /// The context for the format parser, with the detection of headers disabled, as for the other reads of the table:
    /// the parser is reset for every record, so a record would be taken for a header.
    ContextPtr format_context;
    const StreamSettings stream_settings;
    const size_t max_block_size;
    const size_t poll_timeout_ms;
    const StreamingHandleErrorMode handle_error_mode;
    LoggerPtr log;

    const Block non_virtual_header;
    const Block virtual_header;

    /// By inode.
    std::map<UInt64, FileState> files;
    /// The inode of the file to start the next chunk from, so that a file with a backlog does not starve the others.
    UInt64 next_inode = 0;
    /// The time since the last check of the directory.
    Stopwatch poll_watch;
    bool started = false;
    bool stream_finished = false;
};

}
