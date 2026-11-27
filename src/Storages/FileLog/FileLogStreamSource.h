#pragma once

#include <Core/Block.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Core/Streaming/Settings.h>
#include <Interpreters/Context.h>
#include <Processors/ISource.h>
#include <Storages/StorageSnapshot.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>

#include <boost/noncopyable.hpp>

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
/// several names (hard links) is read once. A file is taken for reading by a name that matches the path of the table,
/// and is still read after it is renamed to a name that does not match (`app.log` -> `app.log.1` for the path `*.log`).
/// A file that leaves the directory is closed after its remaining complete records are read.
/// A file truncated in place is read from the beginning. It is detected by its size, or, if it has grown again past
/// the read position, by the contents: the first bytes of the file and the last record read are checked. A file
/// rewritten with the same first bytes and the same record at the same position cannot be told from a file
/// that was appended to.
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

    Status prepare() override;
    void onCancel() noexcept override;
    Chunk generate() override;
    std::optional<Chunk> tryGenerate() override;

private:
    struct FileDescriptor : private boost::noncopyable
    {
        explicit FileDescriptor(int fd_) : fd(fd_) {}
        ~FileDescriptor();

        const int fd;
    };

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
        /// The evidence of the contents that were read: the first bytes of the file up to `position` (at most
        /// `consumed_head_size`) and the hash of the last record read. If the file does not have them any more, it was
        /// truncated and written again, even if it is not shorter than `position`, and it is read from the beginning.
        String head;
        std::optional<UInt64> last_record_hash;
        /// A bounded stream reads only the records that end within this size.
        std::optional<UInt64> bound;
        /// The file is not in the directory any more: it is closed after its remaining complete records are read.
        bool removed = false;
        /// The opened file. Its inode is the key of the state: the file is read through this descriptor,
        /// so a file that gets the name later is never read in its place.
        std::unique_ptr<FileDescriptor> file;
    };

    void updateFiles(bool initial);
    /// Returns false if a name was given to another file while the files were being listed.
    bool tryUpdateFiles(bool initial);
    void reportFinalCursor();
    /// Read the records again from the beginning of the file.
    static void resetPosition(FileState & state);
    /// The file does not have the contents that were read from it any more.
    static bool consumedContentsChanged(const FileState & state);
    static Int64 fingerprint(const FileState & state);

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
    /// The query does not need more rows, but the bounded read of an incremental refresh has to get to its end
    /// to report the cursor: the remaining records are skipped without parsing.
    bool skip_remaining_records = false;
    /// A bounded read for an incremental refresh is pending until it reports its cursor or finishes, so that a target
    /// that commits the cursor with its data waits for it (see `WaitForStreamingCursorTransform`).
    StreamingCursor::PendingReport pending_report;
};

}
