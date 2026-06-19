#pragma once

#include <libnuraft/ptr.hxx>
#include <Common/ThreadPool_fwd.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/SharedMutex.h>

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

    bool deleted = false;

    /// Set under file_mutex (withLock) by the background RemoveChangelog operation IMMEDIATELY before
    /// disk->removeFile(). A reader that resolved a file location outside changelog_lock re-checks this
    /// under the same withLock; if set, it treats the file as compacted-away => the read returns nullptr
    /// (NuRaft snapshot fallback). DISTINCT from `deleted` (above), which is set under writer_mutex/
    /// changelog_lock and read by ChangelogWriter::setFile OUTSIDE file_mutex. Reusing `deleted`
    /// would be a multi-domain data race; this flag lives entirely in the file_mutex domain.
    bool removed_from_disk = false;

    /// Set true (release) after the file is finalized and sealed (complete + !broken_at_end + not the
    /// active writer file). Monotonic false->true. Used by Layer 2 read-ahead to determine whether it is
    /// safe to read a file to physical EOF.
    std::atomic<bool> sealed{false};

    std::deque<std::weak_ptr<ChangelogFileOperation>> file_operations;

    /// How many entries should be stored in this log
    uint64_t expectedEntriesCountInLog() const { return to_log_index - from_log_index + 1; }

    template <typename TFunction>
    void withLock(TFunction && fn)
    {
        std::lock_guard lock(file_mutex);
        fn();
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

/// Transient plan produced by PLAN (under changelog_lock) and consumed by EXECUTE (lock-free).
/// Holds shared_ptr refs to keep file descriptors alive across the lock release.
struct LogReadPlan
{
    /// A coalesced contiguous run of records in ONE file. first_index lets EXECUTE validate against a
    /// torn/overwritten read. FileRuns may target the active writer file too; the append-only invariant
    /// makes that safe — exactly as today's getLogEntriesBetween already does.
    struct FileRun
    {
        ChangelogFileDescriptionPtr file_description;   /// keeps the descriptor object alive across lock release
        size_t position = 0;
        size_t count = 0;
        uint64_t first_index = 0;
    };

    /// L2: open-ended fill cursor for read-ahead (sealed files only, read to EOF).
    /// Not used by Layer 1 EXECUTE; reserved for Layer 2 FILL.
    struct FillCursor
    {
        ChangelogFileDescriptionPtr file_description;
        size_t start_position = 0;
        uint64_t first_index = 0;
    };

    using Item = std::variant<LogEntryPtr, FileRun>;    /// LogEntryPtr = already-resolved cache entry
    std::vector<Item> items;
    std::vector<FillCursor> fill_cursors;   /// L2: sealed-file cursors for the forward window (Layer 2 only)
    size_t reserve_hint = 0;
    bool ok = true;     /// false => a needed location was missing below retained start => EXECUTE returns nullptr
    bool readahead_engaged = false;  /// L2: false => caller uses items via L1 executeReadPlan
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

using PrefetchedCacheEntryPtr = std::shared_ptr<PrefetchedCacheEntry>;
using CacheEntry = std::variant<LogEntryPtr, PrefetchedCacheEntryPtr>;
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
  * Once the log is persisted to the disk, we store its location in the file and allow the storage
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

/// Settings snapshot for the Layer-2 read-ahead subsystem.
/// All values are immutable after snapshot; updated on settings reload.
struct ReadAheadSettings
{
    bool enabled = false;
    uint64_t window_bytes = 64 * 1024 * 1024;
    uint64_t max_peer_readers = 8;
    uint64_t eviction_timeout_ms = 30000;
    uint64_t pool_threads = 0;             /// 0 = derive from max_peer_readers
    uint64_t serve_wait_timeout_ms = 50;
    bool foreground_fallback = true;
    uint64_t read_request_timeout_ms = 0;  /// 0 = no deadline
};

/// Per-peer read-ahead state. One instance per active follower peer.
/// The single long-lived fill task holds a shared_ptr to this struct so the
/// struct outlives its admission; destroyed when the last shared_ptr drops.
struct PerPeerReader
{
    std::mutex deque_mutex;                         /// leaf; released by cv.wait on park
    std::condition_variable cv;                     /// notify_all only (DD17)
    std::deque<LogEntryPtr> deque;                  /// decoded entries, index-contiguous from deque_front_index
    uint64_t deque_front_index = 0;                 /// index of deque.front()
    uint64_t expected_next = 0;                     /// next index the deque should serve
    size_t deque_bytes = 0;                         /// summed entry buffer sizes; high/low watermark axis
    uint64_t generation = 0;                        /// bumped on rewind/clear; per-chunk fill check
    bool closed = false;                            /// set by retireReader; the fill observes and self-exits
    bool reader_error = false;                      /// terminal decode/IO error → wake consumer, fall back
    bool compacted = false;                         /// removed_from_disk seen → nullptr → snapshot
    bool fallback_waiting = false;                  /// serve sets before taking file_mutex; fill yields within one chunk

    std::vector<LogReadPlan::FillCursor> pending_cursors;  /// handed by serve under deque_mutex

    /// Open-reader model (D3/Direction A): keep the decode stream open across parks.
    /// held_buf is touched ONLY by this reader's single fill task.
    std::unique_ptr<ReadBufferFromFileBase> held_buf;  /// open seekable decode stream kept across parks; nullptr if not open
    DiskPtr opened_disk;                            /// disk held_buf was opened against (identity re-check on resume)
    std::string opened_path;                        /// path held_buf was opened against (identity re-check on resume)
    ChangelogFileDescriptionPtr resume_file;        /// descriptor backing held_buf / to re-open on resume
    uint64_t resume_position = 0;                  /// byte offset of the held stream / to re-seek on re-open
    uint64_t resume_index = 0;                     /// expected first index at resume_position (per-record check)

    /// One-shot fill_done: fulfilled exactly once at terminal exit (closed/removal/decode-error/cache-transition).
    /// Eviction/shutdown wait fill_done OUTSIDE all locks.
    std::promise<void> fill_done_promise;
    std::shared_future<void> fill_done = fill_done_promise.get_future().share();

    std::chrono::steady_clock::time_point last_access;  /// LRU/idle eviction
};

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

    /// PLAN: resolve [start,end) into a LogReadPlan. PRECONDITION: caller holds changelog_lock (shared)
    /// so latest_logs_cache and logs_location are stable; takes commit_logs_cache_mutex (shared) internally
    /// for non-blocking cache PEEKs only. Does NO disk I/O, never resolves a prefetch future, never reads
    /// current_writer, mutates NO shared state. retained_start is the log's first retained index passed
    /// in by the Changelog forwarder (= Changelog::getStartIndex(), which is max_log_id+1 for an EMPTY
    /// store — NOT getFirstIndex(), which returns 0 when empty).
    LogReadPlan getReadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes, uint64_t retained_start) const;

    /// EXECUTE: run the plan's file reads under per-file withLock. Holds NEITHER changelog_lock NOR
    /// commit_logs_cache_mutex. Returns nullptr ONLY when !ok (compacted below start) or a file's
    /// removed_from_disk is set; corruption / index mismatch propagate as exceptions (as today).
    /// NO byte logic here: the byte budget is applied at PLAN time from size_in_file metadata,
    /// so EXECUTE never reads a record merely to learn its size. It reads exactly the planned count records.
    /// read_deadline_ms (DD12): 0 = no deadline; >0 = wall-clock budget checked at each record boundary
    /// via a Stopwatch — on exceed, return the contiguous prefix decoded so far (DD18-valid).
    /// Intentionally called WITHOUT changelog_lock held; see LogReadPlan/PLAN-EXECUTE split.
    LogEntriesPtr executeReadPlan(const LogReadPlan & plan, uint64_t read_deadline_ms = 0) const TSA_NO_THREAD_SAFETY_ANALYSIS;

    LogEntriesPtr getLogEntriesBetween(uint64_t start, uint64_t end, int64_t max_size_bytes = 0) const;

    /// L2: Build a read-ahead plan (fill cursors + bounded items). PRECONDITION: caller holds changelog_lock (shared).
    /// Returns a plan with readahead_engaged=true if read-ahead should be used, false if bypassed to L1.
    LogReadPlan getReadAheadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes, uint64_t retained_start) const;

    /// L2: Serve a request using the per-peer read-ahead path.
    /// PRECONDITION: called WITHOUT changelog_lock (released by the caller before this call).
    /// peer_id identifies the follower peer; plan must have readahead_engaged=true.
    /// Returns the result (possibly nullptr for compaction/snapshot fallback).
    LogEntriesPtr serveReadAhead(int32_t peer_id, const LogReadPlan & plan, uint64_t retained_start) TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// L2: Update the read-ahead settings snapshot. Called when coordination settings are (re)loaded.
    void setReadAheadSettings(ReadAheadSettings settings);

    /// L2: Returns true if read-ahead is currently enabled (lock-free snapshot read).
    bool isReadAheadEnabled() const { return readahead_settings.enabled; }

    void getKeeperLogInfo(KeeperLogInfo & log_info) const;

    bool isConfigLog(uint64_t index) const;

    size_t empty() const;
    size_t size() const;
    size_t getFirstIndex() const;

    void shutdown();
private:
    void prefetchCommitLogs();

    void startCommitLogsPrefetch(uint64_t last_committed_index) const TSA_REQUIRES(commit_logs_cache_mutex);

    bool shouldMoveLogToCommitCache(uint64_t index, size_t log_entry_size) TSA_REQUIRES(commit_logs_cache_mutex);

    void updateTermInfoWithNewEntry(uint64_t index, uint64_t term);

    struct InMemoryCache
    {
        explicit InMemoryCache(size_t size_threshold_, size_t count_threshold_);

        void addEntry(uint64_t index, size_t size, CacheEntry log_entry);
        void addEntry(IndexToCacheEntryNode && node);

        void updateStatsWithNewEntry(uint64_t index, size_t size);

        IndexToCacheEntryNode popOldestEntry();

        bool containsEntry(uint64_t index) const;

        LogEntryPtr getEntry(uint64_t index) const;

        CacheEntry * getCacheEntry(uint64_t index);
        const CacheEntry * getCacheEntry(uint64_t index) const;
        PrefetchedCacheEntryPtr getPrefetchedCacheEntry(uint64_t index);

        void cleanUpTo(uint64_t index);
        void cleanAfter(uint64_t index);

        bool empty() const;
        size_t numberOfEntries() const;
        bool hasSpaceAvailable(size_t log_entry_size) const;
        void clear();

        bool hasUnlimitedSpace() const;

        /// Mapping log_id -> log_entry
        mutable IndexToCacheEntry cache;
        size_t cache_size = 0;
        size_t min_index_in_cache = 0;
        size_t max_index_in_cache = 0;

        const size_t size_threshold;
        const size_t count_threshold;
    };

    InMemoryCache latest_logs_cache;

    mutable SharedMutex commit_logs_cache_mutex;
    mutable InMemoryCache commit_logs_cache TSA_GUARDED_BY(commit_logs_cache_mutex);

    /// Cache optimization: stores max(lastCommittedIndex from getEntry, cleanUpTo parameter).
    /// Invariant: cache is cleaned to at least this index. Used by getEntry to skip
    /// the exclusive lock on commit_logs_cache_mutex when the committed index has not advanced.
    /// Both getEntry and cleanUpTo write to this; writes are conditional (only advance, never regress)
    /// so that an external cleanUpTo with a lower compaction index does not invalidate the optimization.
    /// Reset to 0 in clear().
    mutable std::atomic<uint64_t> last_cleaned_committed_index{0};

    LogEntryPtr latest_config;
    uint64_t latest_config_index = 0;

    mutable LogEntryPtr first_log_entry;
    mutable uint64_t first_log_index = 0;

    std::unique_ptr<ThreadFromGlobalPool> commit_logs_prefetcher;

    struct FileReadInfo
    {
        ChangelogFileDescriptionPtr file_description;
        size_t position{};
        size_t count{};
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

    /// L2 read-ahead subsystem --------------------------------------------------------

    /// Retire (non-blocking terminal retire): mark closed, bump generation, clear fallback_waiting,
    /// notify_all, erase from the map. NO fill_done.wait() — the fill self-exits asynchronously.
    /// Idempotent. Must be called under per_peer_readers_mutex held externally (for the map erase).
    void retireReaderLocked(int32_t peer_id, std::shared_ptr<PerPeerReader> & reader);

    /// One long-lived fill task per reader; runs on readahead_pool.
    void fillTask(std::shared_ptr<PerPeerReader> reader);

    ReadAheadSettings readahead_settings;               /// snapshot of coordination settings
    mutable std::mutex per_peer_readers_mutex;          /// guards the map; NEVER held across a fill join
    std::unordered_map<int32_t, std::shared_ptr<PerPeerReader>> per_peer_readers;
    std::unique_ptr<ThreadPool> readahead_pool;         /// dedicated; built lazily on first engage
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

    /// Return log entries between [start, end) with optional byte size limit
    LogEntriesPtr getLogEntriesBetween(uint64_t start_index, uint64_t end_index, int64_t max_size_bytes = 0);

    /// Layer 1 forwarders: PLAN under changelog_lock, EXECUTE outside.
    LogReadPlan getReadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes = 0);
    /// Intentionally called WITHOUT changelog_lock held; see LogReadPlan/PLAN-EXECUTE split.
    LogEntriesPtr executeReadPlan(const LogReadPlan & plan, uint64_t read_deadline_ms = 0) TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Layer 2 forwarders: read-ahead plan and serve paths.
    /// getReadAheadPlan must be called under changelog_lock (shared).
    LogReadPlan getReadAheadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes, uint64_t retained_start) const;
    /// serveReadAhead must be called WITHOUT changelog_lock held.
    LogEntriesPtr serveReadAhead(int32_t peer_id, const LogReadPlan & plan, uint64_t retained_start) TSA_NO_THREAD_SAFETY_ANALYSIS;
    /// Thread-safe settings update.
    void setReadAheadSettings(ReadAheadSettings settings);
    /// Returns true if read-ahead is currently enabled.
    bool isReadAheadEnabled() const;

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
