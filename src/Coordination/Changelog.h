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

using IndexToOffset = std::unordered_map<uint64_t, off_t>;
using IndexToLogEntry = std::unordered_map<uint64_t, LogEntryPtr>;

enum class ChangelogVersion : uint8_t
{
    V0 = 0,
};

static constexpr auto CURRENT_CHANGELOG_VERSION = ChangelogVersion::V0;

struct ChangelogRecordHeader
{
    ChangelogVersion version = CURRENT_CHANGELOG_VERSION;
    uint64_t index = 0; /// entry log number
    uint64_t term = 0;
    nuraft::log_val_type value_type{};
    uint64_t blob_size = 0;
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
    uint64_t from_log_index;
    uint64_t to_log_index;

    std::string path;
};

class ChangelogWriter;

/// Simplest changelog with files rotation.
/// No compression, no metadata, just entries with headers one by one
/// Able to read broken files/entries and discard them.
class Changelog
{

public:
    Changelog(const std::string & changelogs_dir_, uint64_t rotate_interval_, bool force_sync_, Poco::Logger * log_);

    /// Read changelog from files on changelogs_dir_ skipping all entries before from_log_index
    /// Truncate broken entries, remove files after broken entries.
    void readChangelogAndInitWriter(uint64_t last_commited_log_index, uint64_t logs_to_keep);

    /// Add entry to log with index.
    void appendEntry(uint64_t index, const LogEntryPtr & log_entry);

    /// Write entry at index and truncate all subsequent entries.
    void writeAt(uint64_t index, const LogEntryPtr & log_entry);

    /// Remove log files with to_log_index <= up_to_log_index.
    void compact(uint64_t up_to_log_index);

    uint64_t getNextEntryIndex() const
    {
        return start_index + logs.size();
    }

    uint64_t getStartIndex() const
    {
        return start_index;
    }

    /// Last entry in log, or fake entry with term 0 if log is empty
    LogEntryPtr getLastEntry() const;

    /// Return log entries between [start, end)
    LogEntriesPtr getLogEntriesBetween(uint64_t start_index, uint64_t end_index);

    /// Return entry at position index
    LogEntryPtr entryAt(uint64_t index);

    /// Serialize entries from index into buffer
    BufferPtr serializeEntriesToBuffer(uint64_t index, int32_t count);

    /// Apply entries from buffer overriding existing entries
    void applyEntriesFromBuffer(uint64_t index, nuraft::buffer & buffer);

    /// Fsync latest log to disk and flush buffer
    void flush();

    uint64_t size() const
    {
        return logs.size();
    }

    /// Fsync log to disk
    ~Changelog();

private:
    /// Pack log_entry into changelog record
    static ChangelogRecord buildRecord(uint64_t index, const LogEntryPtr & log_entry);

    /// Starts new file [new_start_log_index, new_start_log_index + rotate_interval]
    void rotate(uint64_t new_start_log_index);

private:
    const std::string changelogs_dir;
    const uint64_t rotate_interval;
    const bool force_sync;
    Poco::Logger * log;

    std::map<uint64_t, ChangelogFileDescription> existing_changelogs;
    std::unique_ptr<ChangelogWriter> current_writer;
    IndexToOffset index_to_start_pos;
    IndexToLogEntry logs;
    uint64_t start_index = 0;
};

}
