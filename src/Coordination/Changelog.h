#pragma once

#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <city.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/HashingWriteBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Disks/IDisk.h>

namespace DB
{

using Checksum = UInt64;

using LogEntryPtr = nuraft::ptr<nuraft::log_entry>;
using LogEntries = std::vector<LogEntryPtr>;
using LogEntriesPtr = nuraft::ptr<LogEntries>;
using BufferPtr = nuraft::ptr<nuraft::buffer>;

using IndexToOffset = std::unordered_map<size_t, off_t>;
using IndexToLogEntry = std::unordered_map<size_t, LogEntryPtr>;

enum class ChangelogVersion : uint8_t
{
    V0 = 0,
};

static constexpr auto CURRENT_CHANGELOG_VERSION = ChangelogVersion::V0;

struct ChangelogRecordHeader
{
    ChangelogVersion version = CURRENT_CHANGELOG_VERSION;
    size_t index; /// entry log number
    size_t term;
    nuraft::log_val_type value_type;
    size_t blob_size;
};

/// Changelog record on disk
struct ChangelogRecord
{
    ChangelogRecordHeader header;
    nuraft::ptr<nuraft::buffer> blob;
};

/// changelog_fromindex_toindex.bin
/// [fromindex, toindex] <- inclusive
struct ChangelogFileDescription
{
    std::string prefix;
    size_t from_log_index;
    size_t to_log_index;

    std::string path;
};

class ChangelogWriter;

/// Simplest changelog with files rotation.
/// No compression, no metadata, just entries with headers one by one
/// Able to read broken files/entries and discard them.
class Changelog
{

public:
    Changelog(const std::string & changelogs_dir_, size_t rotate_interval_, Poco::Logger * log_);

    /// Read changelog from files on changelogs_dir_ skipping all entries before from_log_index
    /// Truncate broken entries, remove files after broken entries.
    void readChangelogAndInitWriter(size_t from_log_index);

    /// Add entry to log with index. Call fsync if force_sync true.
    void appendEntry(size_t index, const LogEntryPtr & log_entry, bool force_sync);

    /// Write entry at index and truncate all subsequent entries.
    void writeAt(size_t index, const LogEntryPtr & log_entry, bool force_sync);

    /// Remove log files with to_log_index <= up_to_log_index.
    void compact(size_t up_to_log_index);

    size_t getNextEntryIndex() const
    {
        return start_index + logs.size();
    }

    size_t getStartIndex() const
    {
        return start_index;
    }

    /// Last entry in log, or fake entry with term 0 if log is empty
    LogEntryPtr getLastEntry() const;

    /// Return log entries between [start, end)
    LogEntriesPtr getLogEntriesBetween(size_t start_index, size_t end_index);

    /// Return entry at position index
    LogEntryPtr entryAt(size_t index);

    /// Serialize entries from index into buffer
    BufferPtr serializeEntriesToBuffer(size_t index, int32_t count);

    /// Apply entries from buffer overriding existing entries
    void applyEntriesFromBuffer(size_t index, nuraft::buffer & buffer, bool force_sync);

    /// Fsync log to disk
    void flush();

    size_t size() const
    {
        return logs.size();
    }

    /// Fsync log to disk
    ~Changelog();

private:
    /// Pack log_entry into changelog record
    static ChangelogRecord buildRecord(size_t index, const LogEntryPtr & log_entry);

    /// Starts new file [new_start_log_index, new_start_log_index + rotate_interval]
    void rotate(size_t new_start_log_index);

private:
    const std::string changelogs_dir;
    const size_t rotate_interval;
    Poco::Logger * log;

    std::map<size_t, ChangelogFileDescription> existing_changelogs;
    std::unique_ptr<ChangelogWriter> current_writer;
    IndexToOffset index_to_start_pos;
    IndexToLogEntry logs;
    size_t start_index = 0;
};

}
