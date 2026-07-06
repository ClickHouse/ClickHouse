#pragma once

#include <libnuraft/ptr.hxx>
#include <Common/ThreadPool_fwd.h>
#include <Common/ConcurrentBoundedQueue.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <variant>
#include <unordered_map>
#include <unordered_set>
#include <future>
#include <vector>
#include <filesystem>

namespace nuraft
{
    struct log_entry;
    struct buffer;
    struct raft_server;
}

namespace DB
{
    class ReadBuffer;
    class ReadBufferFromFileBase;
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

struct ChangelogFileOperation;
using ChangelogFileOperationPtr = std::shared_ptr<ChangelogFileOperation>;

/// changelog_fromindex_toindex.bin
/// [fromindex, toindex] <- inclusive
struct ChangelogFileDescription
{
    std::string prefix;
    uint64_t from_log_index{};
    uint64_t to_log_index{};
    std::string extension;

    DiskPtr disk;
    std::string path;

    bool broken_at_end = false;

    std::mutex file_mutex;

    bool marked_as_deleted = false;

    /// Set under file_mutex just before disk->removeFile(). Unlike `marked_as_deleted` (scheduled but
    /// may still be on disk), this means the file is actually gone. Readers re-check this and return nullptr.
    bool removed_from_disk = false;

    /// Maximal index-consecutive, physically contiguous spans of current (non-superseded) records in
    /// this file. A rewrite leaves the old records on disk and starts a new run at the append
    /// position. Read-ahead planners use this to bound fill cursors to a single run.
    /// Mutated only under the exclusive changelog_lock, read under the shared lock. Fill tasks run
    /// without that lock and must never touch this. Not persisted; rebuilt by the init scan.
    struct ValidRuns
    {
        struct Run
        {
            size_t start_position = 0;  /// == logs_location[first_index].position
            uint64_t first_index = 0;
        };

        /// Sorted by first_index; run i covers [runs[i].first_index, runs[i+1].first_index), the last
        /// covers [runs.back().first_index, end_index). No index gap between consecutive runs.
        std::vector<Run> runs;
        /// One-past the last located index recorded into this file. 0 = no runs.
        uint64_t end_index = 0;
        /// Physical one-past-end of the last recorded record (contiguity check for extension).
        size_t end_position = 0;

        void addLocatedRecord(uint64_t index, size_t position, size_t size_in_file);
        /// Keep only claims below index (run containing index-1 survives, truncated implicitly).
        void truncateAt(uint64_t index, size_t new_end_position);
        void clear();
    };

    ValidRuns valid_runs;

    std::deque<std::weak_ptr<ChangelogFileOperation>> file_operations;

    /// How many entries should be stored in this log
    uint64_t expectedEntriesCountInLog() const { return to_log_index - from_log_index + 1; }

    template <typename TFunction>
    decltype(auto) withLock(TFunction && fn)
    {
        std::lock_guard lock(file_mutex);
        return fn();
    }

    std::string getPathSafe()
    {
        std::lock_guard lock(file_mutex);
        return path;
    }

    void waitAllAsyncOperations();
};

using ChangelogFileDescriptionPtr = std::shared_ptr<ChangelogFileDescription>;

struct KeeperChangelogStatus
{
    uint64_t from_log_index;
    uint64_t to_log_index;
    std::optional<uint64_t> last_entry_index;
    String path;
    DiskPtr disk;
    bool is_compressed;
    bool active;
    bool is_broken;
};

class ChangelogWriter;

struct LogFileSettings
{
    bool force_sync = true;
    bool compress_logs = true;
    uint64_t rotate_interval = 100000;
    uint64_t max_size = 0;
    uint64_t overallocate_size = 0;
    uint64_t latest_logs_cache_size_threshold = 0;
    uint64_t latest_logs_cache_entry_count_threshold = 0;
    uint64_t commit_logs_cache_size_threshold = 0;
    uint64_t commit_logs_cache_entry_count_threshold = 0;
};

struct FlushSettings
{
    uint64_t max_flush_batch_size = 1000;
};

struct LogLocation
{
    ChangelogFileDescriptionPtr file_description;
    size_t position;
    size_t entry_size;
    size_t size_in_file;
};

/// Read plan built under changelog_lock and executed without it.
/// Holds shared_ptr refs to keep file descriptors alive across the lock release.
struct LogReadPlan
{
    /// File position descriptor shared by direct reads and read-ahead fill cursors.
    /// count > 0 always: exact record count for direct reads; bounds fill cursors to a single valid
    /// run's flushed records (safe on the active file).
    struct FileSpan
    {
        ChangelogFileDescriptionPtr file_description;
        size_t position = 0;
        uint64_t first_index = 0;
        size_t count = 0;
    };

    /// Speculative fill cursors. Present only when read-ahead is engaged.
    using ReadAheadWindow = std::deque<FileSpan>;

    using Item = std::variant<LogEntryPtr, FileSpan>;   /// LogEntryPtr = already in cache
    std::vector<Item> items;
    std::optional<ReadAheadWindow> read_ahead_window;   /// present ⇒ use serveReadAhead
    uint64_t start_index = 0;
    size_t requested_entry_count = 0; /// = end - start; a reservation hint, not a positional anchor
    bool logs_compacted = false;      /// true => log compacted below requested start; caller gets nullptr
};

using IndexToLogEntry = std::unordered_map<uint64_t, LogEntryPtr>;

/**
  * Storage for storing and handling deserialized entries from disk.
  * It consists of an in-memory cache and two decoded read-ahead subsystems, relying on the fact
  * that entries are read sequentially in Raft (replication, committing) rather than randomly, so
  * an LRU/SLRU-style cache would not help.
  *
  * The latest logs cache holds the most recent logs in memory (unflushed tail plus a flushed
  * suffix), bounded by latest_logs_cache_size_threshold; once persisted, its location is recorded
  * (logs_location) and the entry may be evicted.
  *
  * Replication is served by per-peer read-ahead readers (per_peer_readers): each follower gets a
  * dedicated reader decoding entries ahead of the requested range, bounded by keeper_log_readahead_*.
  *
  * Committing is served by a single always-on commit read-ahead reader (commit_reader), built on
  * the same machinery but exempt from peer capacity limits and idle eviction, with its window sized
  * by commit_logs_cache_size_threshold. Lookup order: in-memory hits -> commit reader fast-path pop
  * -> on miss, build a plan and bounded-wait for the reader -> blocking direct read as fallback.
  *
  * Both planners derive fill cursors from per-file valid-run metadata (ChangelogFileDescription::
  * valid_runs, appendRunCursors), bounding every cursor to a single run of current records. This
  * covers the active file's flushed prefix too, so peer read-ahead needs no separate seal bit.
  */

/// Settings for the per-peer decoded changelog read-ahead subsystem.
struct ReadAheadSettings
{
    bool enabled = false;
    uint64_t window_bytes = 64 * 1024 * 1024;
    uint64_t max_peer_readers = 8;
    uint64_t eviction_timeout_ms = 30000;
    uint64_t pool_threads = 0; /// 0 = derived from max_peer_readers
    uint64_t serve_wait_timeout_ms = 50;
    uint64_t chunk_size = 16;
};

/// Read-ahead state for one follower peer. Shared between the serve path and the fill task.
/// Full definition in Changelog.cpp — only the forward declaration is needed by the header.
struct PerPeerReader;

struct LogEntryStorage
{
    LogEntryStorage(const LogFileSettings & log_settings, ReadAheadSettings readahead_settings_, KeeperContextPtr keeper_context_);

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

    /// Build a read plan for [start, end). Must be called under changelog_lock (shared). No disk I/O.
    /// retained_start is the first retained index (= Changelog::getStartIndex()).
    LogReadPlan getReadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes, uint64_t retained_start) const;

    /// Execute a read plan without holding changelog_lock. Returns nullptr if the log was compacted
    /// below start or a file was removed from disk. read_deadline_ms=0 means no deadline; if exceeded,
    /// returns the prefix decoded so far.
    /// TSA: changelog_lock released by caller after plan build; file lifetime held by ChangelogFileDescriptionPtr
    /// refs in the plan.
    LogEntriesPtr executeReadPlan(const LogReadPlan & plan, uint64_t read_deadline_ms = 0) const TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Build a read-ahead plan. Must be called under changelog_lock (shared).
    /// Returns a plan with read_ahead_window set if read-ahead is active, absent to fall back to executeReadPlan.
    LogReadPlan getReadAheadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes, uint64_t retained_start) const;

    /// Serve a read-ahead request for peer_id. Must be called without changelog_lock.
    /// Returns nullptr on compaction or snapshot fallback.
    /// TSA: takes per_peer_readers_mutex briefly then releases; fill/serve coordination under PerPeerReader::deque_mutex.
    LogEntriesPtr serveReadAhead(int32_t peer_id, const LogReadPlan & plan) TSA_NO_THREAD_SAFETY_ANALYSIS;

    bool isReadAheadEnabled() const { return readahead_settings.enabled; }

    /// In-memory hits only (no disk IO, no mutation). Caller holds changelog_lock (shared).
    LogEntryPtr getEntryFromMemory(uint64_t index) const;

    /// Commit read-ahead fast path: pop the commit reader's deque front iff it equals index.
    LogEntryPtr tryPopCommitReadAhead(uint64_t index);

    /// Build a single-entry commit plan with bounded fill cursors. Under changelog_lock (shared).
    LogReadPlan getCommitReadPlan(uint64_t index, uint64_t retained_start) const;

    /// Serve a commit read: install cursors, bounded wait, blocking fallback read. Called without
    /// changelog_lock; returns nullptr only if the entry is genuinely gone.
    LogEntryPtr serveCommitEntry(uint64_t index, const LogReadPlan & plan) TSA_NO_THREAD_SAFETY_ANALYSIS;

    void getKeeperLogInfo(KeeperLogInfo & log_info) const;

    bool isConfigLog(uint64_t index) const;

    size_t empty() const;
    size_t size() const;
    size_t getFirstIndex() const;

    void shutdown();

    /// Test-only: verify valid-run metadata is consistent with logs_location. Must be called while
    /// the instance is quiescent.
    void checkValidRunsConsistency() const;
private:
    void updateTermInfoWithNewEntry(uint64_t index, uint64_t term);

    struct InMemoryCache
    {
        explicit InMemoryCache(size_t size_threshold_, size_t count_threshold_);

        void addEntry(uint64_t index, size_t size, LogEntryPtr log_entry);

        void updateStatsWithNewEntry(uint64_t index, size_t size);

        void popOldestEntry();

        bool containsEntry(uint64_t index) const;

        LogEntryPtr getEntry(uint64_t index) const;

        void cleanUpTo(uint64_t index);
        void cleanAfter(uint64_t index);

        bool empty() const;
        size_t numberOfEntries() const;
        bool hasSpaceAvailable(size_t log_entry_size) const;
        void clear();

        bool hasUnlimitedSpace() const;

        /// Mapping log_id -> log_entry
        IndexToLogEntry cache;
        size_t cache_size = 0;
        size_t min_index_in_cache = 0;
        size_t max_index_in_cache = 0;

        const size_t size_threshold;
        const size_t count_threshold;
    };

    InMemoryCache latest_logs_cache;

    LogEntryPtr latest_config;
    uint64_t latest_config_index = 0;

    mutable LogEntryPtr first_log_entry;
    mutable uint64_t first_log_index = 0;

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

    std::atomic<bool> is_shutdown{false};
    KeeperContextPtr keeper_context;
    LoggerPtr log;

    /// Append bounded fill cursors covering `file`'s valid runs over [from_index, end_limit).
    /// PRECONDITION: caller holds changelog_lock (shared); from_index is located and its location is in
    /// `file`. Returns the exclusive end of the emitted coverage (== from_index when nothing to emit).
    uint64_t appendRunCursors(
        LogReadPlan::ReadAheadWindow & window,
        const ChangelogFileDescriptionPtr & file,
        uint64_t from_index,
        uint64_t end_limit) const;

    /// Mark a reader closed and remove it from the map. Fill task self-exits asynchronously.
    void retireReaderLocked(int32_t peer_id, const std::shared_ptr<PerPeerReader> & reader) TSA_REQUIRES(per_peer_readers_mutex);

    /// Close and discard all per-peer readers. Called on writeAt to invalidate stale decoded content.
    void closeAllReadersLocked();

    /// Lazily create the read-ahead thread pool.
    void ensureReadAheadPoolLocked() TSA_REQUIRES(per_peer_readers_mutex);

    /// Evict idle readers. Gated by map capacity and a wall-clock interval to avoid scanning on every call.
    void evictIdleReadersLocked(std::chrono::steady_clock::time_point now) TSA_REQUIRES(per_peer_readers_mutex);

    /// Reap a terminal reader for peer_id (if any), enforce max_peer_readers, and create/schedule a new
    /// reader. Returns nullptr when at capacity (caller should fall back to direct read).
    std::shared_ptr<PerPeerReader> acquireReaderLocked(int32_t peer_id, const LogReadPlan & plan, std::chrono::steady_clock::time_point now)
        TSA_REQUIRES(per_peer_readers_mutex);

    /// Create a fresh reader and schedule its fill task. Returns nullptr if the pool is unavailable
    /// (post-shutdown). Shared by per-peer and commit-reader acquisition; does not touch the maps.
    std::shared_ptr<PerPeerReader> makeReaderLocked(uint64_t start_index, size_t budget_bytes, std::chrono::steady_clock::time_point now)
        TSA_REQUIRES(per_peer_readers_mutex);

    /// Install the new plan into an existing reader (rewind if non-sequential, push fill cursors).
    /// Caller holds reader.deque_mutex.
    void installPlanLocked(PerPeerReader & reader, const LogReadPlan & plan);

    /// Serve items from reader's deque, falling back to direct read for any tail that is not available.
    LogEntriesPtr drainReader(int32_t peer_id, const std::shared_ptr<PerPeerReader> & reader, const LogReadPlan & plan);

    /// Background fill task for one peer reader.
    void fillTask(std::shared_ptr<PerPeerReader> reader) const;

    ReadAheadSettings readahead_settings;
    mutable std::mutex per_peer_readers_mutex;
    std::unordered_map<int32_t, std::shared_ptr<PerPeerReader>> per_peer_readers TSA_GUARDED_BY(per_peer_readers_mutex);
    std::unique_ptr<ThreadPool> readahead_pool TSA_GUARDED_BY(per_peer_readers_mutex);
    std::chrono::steady_clock::time_point last_eviction_scan TSA_GUARDED_BY(per_peer_readers_mutex);

    /// Sentinel peer id for the commit reader (logging + drainReader retire routing).
    static constexpr int32_t COMMIT_READER_ID = -2;
    /// Always-on reader feeding the NuRaft commit loop; owned separately from per_peer_readers,
    /// exempt from max_peer_readers capacity and idle eviction.
    std::shared_ptr<PerPeerReader> commit_reader TSA_GUARDED_BY(per_peer_readers_mutex);
    /// Commit reader window byte budget (LogFileSettings.commit_logs_cache_size_threshold).
    /// commit_logs_cache_entry_count_threshold has no effect: a sequentially-drained window
    /// gains nothing from an entry-count bound.
    size_t commit_readahead_window_bytes = 0;

    std::shared_ptr<PerPeerReader> acquireCommitReaderLocked(const LogReadPlan & plan, std::chrono::steady_clock::time_point now)
        TSA_REQUIRES(per_peer_readers_mutex);
    void retireCommitReaderLocked() TSA_REQUIRES(per_peer_readers_mutex);
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
        ReadAheadSettings readahead_settings,
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

    /// Must be called under changelog_lock.
    LogReadPlan getReadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes = 0);
    /// Must be called without changelog_lock.
    LogEntriesPtr executeReadPlan(const LogReadPlan & plan, uint64_t read_deadline_ms = 0) TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Must be called under changelog_lock.
    LogReadPlan getReadAheadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes) const;
    /// Must be called without changelog_lock.
    LogEntriesPtr serveReadAhead(int32_t peer_id, const LogReadPlan & plan) TSA_NO_THREAD_SAFETY_ANALYSIS;
    bool isReadAheadEnabled() const;

    /// In-memory hits only (no disk IO, no mutation). Caller holds changelog_lock (shared).
    LogEntryPtr entryFromMemory(uint64_t index) const;
    /// Commit read-ahead fast path: pop the commit reader's deque front iff it equals index.
    LogEntryPtr tryPopCommitReadAhead(uint64_t index);
    /// Build a single-entry commit plan with bounded fill cursors. Under changelog_lock (shared).
    LogReadPlan getCommitReadPlan(uint64_t index) const;
    /// Serve a commit read: install cursors, bounded wait, blocking fallback read. Called without
    /// changelog_lock; returns nullptr only if the entry is genuinely gone.
    LogEntryPtr serveCommitEntry(uint64_t index, const LogReadPlan & plan) TSA_NO_THREAD_SAFETY_ANALYSIS;

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

    /// Test-only: forwards to LogEntryStorage::checkValidRunsConsistency.
    void checkValidRunsConsistencyForTests() const { entry_storage.checkValidRunsConsistency(); }

    std::vector<KeeperChangelogStatus> getChangelogsStatus() const;

    static ChangelogFileDescriptionPtr getChangelogFileDescription(const std::filesystem::path & path);

    static void readChangelog(ChangelogFileDescriptionPtr changelog_description, LogEntryStorage & entry_storage);
    static void spliceChangelog(ChangelogFileDescriptionPtr source_changelog, ChangelogFileDescriptionPtr destination_changelog);
    static std::string formatChangelogPath(const std::string & name_prefix, uint64_t from_index, uint64_t to_index, const std::string & extension);

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

    /// Remove all changelogs from disk with start_index bigger than remove_after_log_start_index
    void removeAllLogsAfter(uint64_t remove_after_log_start_index);
    /// Remove all changelogs from disk with start index smaller than remove_before_log_start_index
    void removeAllLogFilesBefore(uint64_t remove_before_log_start_index);
    /// Remove all logs from disk
    void removeAllLogs();
    /// Init writer for existing log with some entries already written
    void initWriter(ChangelogFileDescriptionPtr description);

    /// Thread for operations on changelog file, e.g. removing the file
    void backgroundChangelogOperationsThread();

    void modifyChangelogAsync(ChangelogFileOperationPtr changelog_operation);
    void removeChangelogAsync(ChangelogFileDescriptionPtr changelog);
    void moveChangelogAsync(ChangelogFileDescriptionPtr changelog, std::string new_path, DiskPtr new_disk);

    const String changelogs_detached_dir;
    const uint64_t rotate_interval;
    const bool compress_logs;
    LoggerPtr log;

    mutable std::mutex writer_mutex;
    /// Current writer for changelog file
    std::unique_ptr<ChangelogWriter> current_writer;

    LogEntryStorage entry_storage;

    std::atomic<uint64_t> max_log_id{0};

    ConcurrentBoundedQueue<ChangelogFileOperationPtr> changelog_operation_queue{std::numeric_limits<size_t>::max()};
    std::unique_ptr<ThreadFromGlobalPool> background_changelog_operations_thread;

    struct AppendLog
    {
        uint64_t index{};
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
