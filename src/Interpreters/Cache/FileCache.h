#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <boost/functional/hash.hpp>

#include <IO/ReadSettings.h>

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Cache/LockedFileCachePriority.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/QueryLimit.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

struct LockedKeyMetadata;
using LockedKeyMetadataPtr = std::shared_ptr<LockedKeyMetadata>;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
/// Different caching algorithms are implemented using IFileCachePriority.
class FileCache : private boost::noncopyable
{
public:
    using Key = DB::FileCacheKey;
    using QueryLimit = DB::FileCacheQueryLimit;

    FileCache(const String & cache_base_path_, const FileCacheSettings & cache_settings_);

    ~FileCache();

    void initialize();

    const String & getBasePath() const { return cache_base_path; }

    static Key createKeyForPath(const String & path);

    /**
     * Given an `offset` and `size` representing [offset, offset + size) bytes interval,
     * return list of cached non-overlapping non-empty
     * file segments `[segment1, ..., segmentN]` which intersect with given interval.
     *
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * As long as pointers to returned file segments are hold
     * it is guaranteed that these file segments are not removed from cache.
     */
    FileSegmentsHolderPtr getOrSet(const Key & key, size_t offset, size_t size, const CreateFileSegmentSettings & settings);

    /**
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * If file segment has state EMPTY, then it is also marked as "detached". E.g. it is "detached"
     * from cache (not owned by cache), and as a result will never change it's state and will be destructed
     * with the destruction of the holder, while in getOrSet() EMPTY file segments can eventually change
     * it's state (and become DOWNLOADED).
     */
    FileSegmentsHolderPtr get(const Key & key, size_t offset, size_t size);

    FileSegmentsHolderPtr set(const Key & key, size_t offset, size_t size, const CreateFileSegmentSettings & settings);

    /// Remove files by `key`. Removes files which might be used at the moment.
    void removeKeyIfExists(const Key & key);

    /// Remove files by `key`. Will not remove files which are used at the moment.
    void removeAllReleasable();

    String getPathInLocalCache(const Key & key, size_t offset, FileSegmentKind segment_kind) const;

    String getPathInLocalCache(const Key & key) const;

    std::vector<String> tryGetCachePaths(const Key & key);

    size_t getUsedCacheSize() const;

    size_t getFileSegmentsNum() const;

    size_t getMaxFileSegmentSize() const { return max_file_segment_size; }

    bool tryReserve(const FileSegment & file_segment, size_t size);

    FileSegmentsHolderPtr getSnapshot();

    FileSegmentsHolderPtr getSnapshot(const Key & key);

    FileSegmentsHolderPtr dumpQueue();

    bool isInitialized() const { return is_initialized; }

    CacheGuard::Lock cacheLock() { return cache_guard.lock(); }

    void cleanup();

    void deactivateBackgroundOperations();

    LockedKeyMetadataPtr lockKeyMetadata(const Key & key, KeyMetadataPtr key_metadata, bool return_null_if_in_cleanup_queue = true) const;

    /// For per query cache limit.
    struct QueryContextHolder : private boost::noncopyable
    {
        QueryContextHolder(const String & query_id_, FileCache * cache_, QueryLimit::QueryContextPtr context_);

        QueryContextHolder() = default;

        ~QueryContextHolder();

        String query_id;
        FileCache * cache = nullptr;
        QueryLimit::QueryContextPtr context;
    };
    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;
    QueryContextHolderPtr getQueryContextHolder(const String & query_id, const ReadSettings & settings);

private:
    using KeyAndOffset = FileCacheKeyAndOffset;

    const String cache_base_path;
    const size_t max_file_segment_size;
    const bool allow_persistent_files;
    const size_t bypass_cache_threshold = 0;
    const size_t delayed_cleanup_interval_ms;

    Poco::Logger * log;

    std::exception_ptr init_exception;
    std::atomic<bool> is_initialized = false;
    mutable std::mutex init_mutex;

    CacheMetadata metadata;

    FileCachePriorityPtr main_priority;
    mutable CacheGuard cache_guard;

    struct HitsCountStash
    {
        HitsCountStash(size_t hits_threashold_, size_t queue_size_)
            : hits_threshold(hits_threashold_), queue(std::make_unique<LRUFileCachePriority>(0, queue_size_))
        {
            if (!queue_size_)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Queue size for hits queue must be non-zero");
        }

        const size_t hits_threshold;
        FileCachePriorityPtr queue;
        using Records = std::unordered_map<KeyAndOffset, IFileCachePriority::Iterator, FileCacheKeyAndOffsetHash>;
        Records records;
    };

    mutable std::unique_ptr<HitsCountStash> stash;

    FileCacheQueryLimitPtr query_limit;

    void assertInitialized() const;

    void loadMetadata();

    FileSegments getImpl(const LockedKeyMetadata & locked_key, const FileSegment::Range & range);

    FileSegments splitRangeInfoFileSegments(
        LockedKeyMetadata & locked_key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        const CreateFileSegmentSettings & create_settings);

    void fillHolesWithEmptyFileSegments(
        LockedKeyMetadata & locked_key,
        FileSegments & file_segments,
        const FileSegment::Range & range,
        bool fill_with_detached_file_segments,
        const CreateFileSegmentSettings & settings);

    KeyMetadata::iterator addFileSegment(
        LockedKeyMetadata & locked_key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        const CreateFileSegmentSettings & create_settings,
        const CacheGuard::Lock *);

    static void removeFileSegment(
        LockedKeyMetadata & locked_key,
        FileSegmentPtr file_segment,
        const CacheGuard::Lock &);

    bool tryReserveImpl(
        const FileSegment & file_segment,
        size_t size,
        IFileCachePriority & priority_queue,
        QueryLimit::LockedQueryContext * query_context,
        const CacheGuard::Lock &);

    struct IterateAndLockResult
    {
        IFileCachePriority::IterationResult iteration_result;
        bool lock_key = false;
    };

    void assertCacheCorrectness();

    BackgroundSchedulePool::TaskHolder cleanup_task;

    void cleanupThreadFunc();
};

}
