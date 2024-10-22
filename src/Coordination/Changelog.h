#pragma once

#include <libnuraft/ptr.hxx>
#include <Common/ThreadPool_fwd.h>
#include <Common/ConcurrentBoundedQueue.h>

#include <map>
#include <variant>
#include <unordered_map>
#include <unordered_set>
#include <future>

namespace nuraft
{
    struct log_entry;
    struct buffer;
    struct raft_server;
}

namespace Poco
{
    class Logger;
}

using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace DB
{

using Checksum = uint64_t;

using LogEntryPtr = nuraft::ptr<nuraft::log_entry>;
using LogEntries = std::vector<LogEntryPtr>;
using LogEntriesPtr = nuraft::ptr<LogEntries>;
using BufferPtr = nuraft::ptr<nuraft::buffer>;

struct KeeperLogInfo;
class KeeperContext;
using KeeperContextPtr = std::shared_ptr<KeeperContext>;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

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

    std::mutex file_mutex;

    bool deleted = false;

    /// How many entries should be stored in this log
    uint64_t expectedEntriesCountInLog() const { return to_log_index - from_log_index + 1; }

    template <typename TFunction>
    void withLock(TFunction && fn)
    {
        std::lock_guard lock(file_mutex);
        fn();
    }
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
    uint64_t latest_logs_cache_size_threshold = 0;
    uint64_t commit_logs_cache_size_threshold = 0;
};

struct FlushSettings
{
    uint64_t max_flush_batch_size = 1000;
};

struct LogLocation
{
    ChangelogFileDescriptionPtr file_description;
    size_t position;
    size_t size;
};

struct PrefetchedCacheEntry
{
    explicit PrefetchedCacheEntry();

    const LogEntryPtr & getLogEntry() const;
    void resolve(std::exception_ptr exception);
    void resolve(LogEntryPtr log_entry_);
private:
    std::promise<LogEntryPtr> log_entry_resolver;
    mutable std::shared_future<LogEntryPtr> log_entry;
};

using CacheEntry = std::variant<LogEntryPtr, PrefetchedCacheEntry>;
using IndexToCacheEntry = std::unordered_map<uint64_t, CacheEntry>;
using IndexToCacheEntryNode = typename IndexToCacheEntry::node_type;

/**
  * Storage for storing and handling deserialized entries from disk.
  * It consists of 2 in-memory caches that rely heavily on the way
  * entries are used in Raft.
  * Random and repeated access to certain entries is almost never done so we can't implement a solution
  * like LRU/SLRU cache because entries would be cached and never read again.
  * Entries are often read sequentially for 2 cases:
  * - for replication
  * - for committing
  *
  * First cache will store latest logs in memory, limited by the latest_logs_cache_size_threshold coordination setting.
  * Once the log is persisted to the disk, we store it's location in the file and allow the storage
  * to evict that log from cache if it's needed.
  * Latest logs cache should have a high hit rate in "normal" operation for both replication and committing.
  *
  * As we commit (and read) logs sequentially, we will try to read from latest logs cache.
  * In some cases, latest logs could be ahead from last committed log by more than latest_logs_cache_size_threshold
  * which means that for each commit we would need to read the log from disk.
  * In case latest logs cache hits the threshold we have a second cache called commit logs cache limited by commit_logs_cache_size_threshold.
  * If a log is evicted from the latest logs cache, we check if we can move it to commit logs cache to avoid re-reading the log from disk.
  * If latest logs cache moves ahead of the commit log by a lot or commit log hits the threshold
  * we cannot move the entries from latest logs and we will need to refill the commit cache from disk.
  * To avoid reading entry by entry (which can have really bad effect on performance because we support disks based on S3),
  * we try to prefetch multiple entries ahead of time because we know that they will be read by commit thread
  * in the future.
  * Commit logs cache should have a high hit rate if we start with a lot of unprocessed logs that cannot fit in the
  * latest logs cache.
  */
struct LogEntryStorage
{
    LogEntryStorage(const LogFileSettings & log_settings, KeeperContextPtr keeper_context_);

    ~LogEntryStorage();

    void addEntry(uint64_t index, const LogEntryPtr & log_entry);
    void addEntryWithLocation(uint64_t index, const LogEntryPtr & log_entry, LogLocation log_location);
    /// clean all logs up to (but not including) index
    void cleanUpTo(uint64_t index);
    /// clean all logs after (but not including) index
    void cleanAfter(uint64_t index);
    bool contains(uint64_t index) const;
    LogEntryPtr getEntry(uint64_t index) const;
    void clear();
    LogEntryPtr getLatestConfigChange() const;
    uint64_t termAt(uint64_t index) const;

    using IndexWithLogLocation = std::pair<uint64_t, LogLocation>;

    void addLogLocations(std::vector<IndexWithLogLocation> && indices_with_log_locations);

    void refreshCache();

    LogEntriesPtr getLogEntriesBetween(uint64_t start, uint64_t end) const;

    void getKeeperLogInfo(KeeperLogInfo & log_info) const;

    bool isConfigLog(uint64_t index) const;

    size_t empty() const;
    size_t size() const;
    size_t getFirstIndex() const;

    void shutdown();
private:
    void prefetchCommitLogs();

    void startCommitLogsPrefetch(uint64_t last_committed_index) const;

    bool shouldMoveLogToCommitCache(uint64_t index, size_t log_entry_size);

    void updateTermInfoWithNewEntry(uint64_t index, uint64_t term);

    struct InMemoryCache
    {
        explicit InMemoryCache(size_t size_threshold_);

        void addEntry(uint64_t index, size_t size, CacheEntry log_entry);
        void addEntry(IndexToCacheEntryNode && node);

        void updateStatsWithNewEntry(uint64_t index, size_t size);

        IndexToCacheEntryNode popOldestEntry();

        bool containsEntry(uint64_t index) const;

        LogEntryPtr getEntry(uint64_t index) const;

        CacheEntry * getCacheEntry(uint64_t index);
        const CacheEntry * getCacheEntry(uint64_t index) const;
        PrefetchedCacheEntry & getPrefetchedCacheEntry(uint64_t index);

        void cleanUpTo(uint64_t index);
        void cleanAfter(uint64_t index);

        bool empty() const;
        size_t numberOfEntries() const;
        bool hasSpaceAvailable(size_t log_entry_size) const;
        void clear();

        /// Mapping log_id -> log_entry
        mutable IndexToCacheEntry cache;
        size_t cache_size = 0;
        size_t min_index_in_cache = 0;
        size_t max_index_in_cache = 0;

        const size_t size_threshold;
    };

    InMemoryCache latest_logs_cache;
    mutable InMemoryCache commit_logs_cache;

    LogEntryPtr latest_config;
    uint64_t latest_config_index = 0;

    mutable LogEntryPtr first_log_entry;
    mutable uint64_t first_log_index = 0;

    std::unique_ptr<ThreadFromGlobalPool> commit_logs_prefetcher;

    struct FileReadInfo
    {
        ChangelogFileDescriptionPtr file_description;
        size_t position;
        size_t count;
    };

    struct PrefetchInfo
    {
        std::vector<FileReadInfo> file_infos;
        std::pair<uint64_t, uint64_t> commit_prefetch_index_range;
        std::atomic<bool> cancel;
        std::atomic<bool> done = false;
    };

    mutable ConcurrentBoundedQueue<std::shared_ptr<PrefetchInfo>> prefetch_queue;
    mutable std::shared_ptr<PrefetchInfo> current_prefetch_info;

    mutable std::mutex logs_location_mutex;
    std::vector<IndexWithLogLocation> unapplied_indices_with_log_locations;
    std::unordered_map<uint64_t, LogLocation> logs_location;
    size_t max_index_with_location = 0;
    size_t min_index_with_location = 0;

    /// store indices of logs that contain config changes
    std::unordered_set<uint64_t> logs_with_config_changes;

    struct LogTermInfo
    {
        uint64_t term = 0;
        uint64_t first_index = 0;
    };

    /// store first index of each term
    /// so we don't have to fetch log to return that information
    /// terms are monotonically increasing so first index is enough
    std::deque<LogTermInfo> log_term_infos;

    bool is_shutdown = false;
    KeeperContextPtr keeper_context;
    LoggerPtr log;
};

/// Simplest changelog with files rotation.
/// No compression, no metadata, just entries with headers one by one.
/// Able to read broken files/entries and discard them. Not thread safe.
class Changelog
{
public:
    Changelog(
        LoggerPtr log_,
        LogFileSettings log_file_settings,
        FlushSettings flush_settings,
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

    uint64_t getNextEntryIndex() const;

    uint64_t getStartIndex() const;

    /// Last entry in log, or fake entry with term 0 if log is empty
    LogEntryPtr getLastEntry() const;

    /// Get entry with latest config in logstore
    LogEntryPtr getLatestConfigChange() const;

    /// Return log entries between [start, end)
    LogEntriesPtr getLogEntriesBetween(uint64_t start_index, uint64_t end_index);

    /// Return entry at position index
    LogEntryPtr entryAt(uint64_t index) const;

    /// Serialize entries from index into buffer
    BufferPtr serializeEntriesToBuffer(uint64_t index, int32_t count);

    /// Apply entries from buffer overriding existing entries
    void applyEntriesFromBuffer(uint64_t index, nuraft::buffer & buffer);

    bool isConfigLog(uint64_t index) const;
    uint64_t termAt(uint64_t index) const;

    /// Fsync latest log to disk and flush buffer
    bool flush();

    std::shared_ptr<bool> flushAsync();

    void shutdown();

    uint64_t size() const;

    uint64_t lastDurableIndex() const
    {
        std::lock_guard lock{durable_idx_mutex};
        return last_durable_idx;
    }

    void setRaftServer(const nuraft::ptr<nuraft::raft_server> & raft_server_);

    bool isInitialized() const;

    void getKeeperLogInfo(KeeperLogInfo & log_info) const;

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
    const bool compress_logs;
    LoggerPtr log;

    std::mutex writer_mutex;
    /// Current writer for changelog file
    std::unique_ptr<ChangelogWriter> current_writer;

    LogEntryStorage entry_storage;

    uint64_t max_log_id = 0;
    /// For compaction, queue of delete not used logs
    /// 128 is enough, even if log is not removed, it's not a problem
    ConcurrentBoundedQueue<std::pair<std::string, DiskPtr>> log_files_to_delete_queue{128};
    std::unique_ptr<ThreadFromGlobalPool> clean_log_thread;

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

    std::unique_ptr<ThreadFromGlobalPool> write_thread;
    ConcurrentBoundedQueue<WriteOperation> write_operations;

    /// Append log completion callback tries to acquire NuRaft's global lock
    /// Deadlock can occur if NuRaft waits for a append/flush to finish
    /// while the lock is taken
    /// For those reasons we call the completion callback in a different thread
    void appendCompletionThread();

    std::unique_ptr<ThreadFromGlobalPool> append_completion_thread;
    ConcurrentBoundedQueue<bool> append_completion_queue;

    // last_durable_index needs to be exposed through const getter so we make mutex mutable
    mutable std::mutex durable_idx_mutex;
    std::condition_variable durable_idx_cv;
    uint64_t last_durable_idx{0};

    nuraft::wptr<nuraft::raft_server> raft_server;

    KeeperContextPtr keeper_context;

    const FlushSettings flush_settings;

    bool initialized = false;
};

}
