#pragma once

#include <optional>
#include <city.h>
#include <Disks/IDisk.h>
#include <IO/CompressionMethod.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <base/defines.h>
#include <libnuraft/nuraft.hxx>
#include <libnuraft/raft_server.hxx>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <Coordination/KeeperContext.h>

namespace DB
{

using Checksum = uint64_t;

using LogEntryPtr = nuraft::ptr<nuraft::log_entry>;
using LogEntries = std::vector<LogEntryPtr>;
using LogEntriesPtr = nuraft::ptr<LogEntries>;
using BufferPtr = nuraft::ptr<nuraft::buffer>;

using IndexToOffset = std::unordered_map<uint64_t, off_t>;
using IndexToLogEntry = std::unordered_map<uint64_t, LogEntryPtr>;

enum class ChangelogVersion : uint8_t
{
    V0 = 0,
    V1 = 1, /// with 64 bit buffer header
    V2 = 2, /// with compression and duplicate records
};

static constexpr auto CURRENT_CHANGELOG_VERSION = ChangelogVersion::V2;

struct ChangelogRecordHeader
{
    ChangelogVersion version = CURRENT_CHANGELOG_VERSION;
    uint64_t index = 0; /// entry log number
    uint64_t term = 0;
    int32_t value_type{};
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
    std::string extension;

    DiskPtr disk;
    std::string path;

    bool deleted = false;

    /// How many entries should be stored in this log
    uint64_t expectedEntriesCountInLog() const { return to_log_index - from_log_index + 1; }
};

using ChangelogFileDescriptionPtr = std::shared_ptr<ChangelogFileDescription>;

class ChangelogWriter;

struct LogFileSettings
{
    bool force_sync = true;
    bool compress_logs = true;
    uint64_t rotate_interval = 100000;
    uint64_t max_size = 0;
    uint64_t overallocate_size = 0;
};

/// Simplest changelog with files rotation.
/// No compression, no metadata, just entries with headers one by one.
/// Able to read broken files/entries and discard them. Not thread safe.
class Changelog
{
public:
    Changelog(
        Poco::Logger * log_,
        LogFileSettings log_file_settings,
        KeeperContextPtr keeper_context_);

    Changelog(Changelog &&) = delete;

    /// Read changelog from files on changelogs_dir_ skipping all entries before from_log_index
    /// Truncate broken entries, remove files after broken entries.
    void readChangelogAndInitWriter(uint64_t last_commited_log_index, uint64_t logs_to_keep);

    /// Add entry to log with index.
    void appendEntry(uint64_t index, const LogEntryPtr & log_entry);

    /// Write entry at index and truncate all subsequent entries.
    void writeAt(uint64_t index, const LogEntryPtr & log_entry);

    /// Remove log files with to_log_index <= up_to_log_index.
    void compact(uint64_t up_to_log_index);

    uint64_t getNextEntryIndex() const { return max_log_id + 1; }

    uint64_t getStartIndex() const { return min_log_id; }

    /// Last entry in log, or fake entry with term 0 if log is empty
    LogEntryPtr getLastEntry() const;

    /// Get entry with latest config in logstore
    LogEntryPtr getLatestConfigChange() const;

    /// Return log entries between [start, end)
    LogEntriesPtr getLogEntriesBetween(uint64_t start_index, uint64_t end_index);

    /// Return entry at position index
    LogEntryPtr entryAt(uint64_t index);

    /// Serialize entries from index into buffer
    BufferPtr serializeEntriesToBuffer(uint64_t index, int32_t count);

    /// Apply entries from buffer overriding existing entries
    void applyEntriesFromBuffer(uint64_t index, nuraft::buffer & buffer);

    /// Fsync latest log to disk and flush buffer
    bool flush();

    std::shared_ptr<bool> flushAsync();

    void shutdown();

    uint64_t size() const { return logs.size(); }

    uint64_t lastDurableIndex() const
    {
        std::lock_guard lock{durable_idx_mutex};
        return last_durable_idx;
    }

    void setRaftServer(const nuraft::ptr<nuraft::raft_server> & raft_server_);

    /// Fsync log to disk
    ~Changelog();

private:
    /// Pack log_entry into changelog record
    static ChangelogRecord buildRecord(uint64_t index, const LogEntryPtr & log_entry);

    DiskPtr getDisk() const;
    DiskPtr getLatestLogDisk() const;

    /// Currently existing changelogs
    std::map<uint64_t, ChangelogFileDescriptionPtr> existing_changelogs;

    using ChangelogIter = decltype(existing_changelogs)::iterator;
    void removeExistingLogs(ChangelogIter begin, ChangelogIter end);

    static void removeLog(const std::filesystem::path & path, const std::filesystem::path & detached_folder);
    /// Remove all changelogs from disk with start_index bigger than start_to_remove_from_id
    void removeAllLogsAfter(uint64_t remove_after_log_start_index);
    /// Remove all logs from disk
    void removeAllLogs();
    /// Init writer for existing log with some entries already written
    void initWriter(ChangelogFileDescriptionPtr description);

    /// Clean useless log files in a background thread
    void cleanLogThread();

    const String changelogs_detached_dir;
    const uint64_t rotate_interval;
    Poco::Logger * log;

    std::mutex writer_mutex;
    /// Current writer for changelog file
    std::unique_ptr<ChangelogWriter> current_writer;
    /// Mapping log_id -> log_entry
    IndexToLogEntry logs;
    /// Start log_id which exists in all "active" logs
    /// min_log_id + 1 == max_log_id means empty log storage for NuRaft
    uint64_t min_log_id = 0;
    uint64_t max_log_id = 0;
    /// For compaction, queue of delete not used logs
    /// 128 is enough, even if log is not removed, it's not a problem
    ConcurrentBoundedQueue<std::pair<std::string, DiskPtr>> log_files_to_delete_queue{128};
    ThreadFromGlobalPool clean_log_thread;

    struct AppendLog
    {
        uint64_t index;
        nuraft::ptr<nuraft::log_entry> log_entry;
    };

    struct Flush
    {
        uint64_t index;
        std::shared_ptr<bool> failed;
    };

    using WriteOperation = std::variant<AppendLog, Flush>;

    void writeThread();

    ThreadFromGlobalPool write_thread;
    ConcurrentBoundedQueue<WriteOperation> write_operations;

    /// Append log completion callback tries to acquire NuRaft's global lock
    /// Deadlock can occur if NuRaft waits for a append/flush to finish
    /// while the lock is taken
    /// For those reasons we call the completion callback in a different thread
    void appendCompletionThread();

    ThreadFromGlobalPool append_completion_thread;
    ConcurrentBoundedQueue<bool> append_completion_queue;

    // last_durable_index needs to be exposed through const getter so we make mutex mutable
    mutable std::mutex durable_idx_mutex;
    std::condition_variable durable_idx_cv;
    uint64_t last_durable_idx{0};

    nuraft::wptr<nuraft::raft_server> raft_server;

    KeeperContextPtr keeper_context;

    bool initialized = false;
};

}
