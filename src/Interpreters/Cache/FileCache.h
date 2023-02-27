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

#include <Interpreters/Cache/LockedFileCachePriority.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/LockedKey.h>
#include <Interpreters/Cache/QueryLimit.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

struct LockedKey;
using LockedKeyPtr = std::shared_ptr<LockedKey>;
using LockedKeysMap = std::unordered_map<FileCacheKey, LockedKeyPtr>;
struct KeysQueue;
using KeysQueuePtr = std::shared_ptr<KeysQueue>;

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

    ~FileCache() = default;

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

    bool tryReserve(const Key & key, size_t offset, size_t size);

    FileSegmentsHolderPtr getSnapshot();

    FileSegmentsHolderPtr getSnapshot(const Key & key);

    FileSegmentsHolderPtr dumpQueue();

    bool isInitialized() const { return is_initialized; }

    void assertCacheCorrectness();

    CacheGuard::Lock cacheLock() { return cache_guard.lock(); }

    LockedKeyPtr createLockedKey(const Key & key, KeyMetadataPtr key_metadata) const;

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

    Poco::Logger * log;

    std::exception_ptr init_exception;
    std::atomic<bool> is_initialized = false;
    mutable std::mutex init_mutex;

    CacheMetadata metadata;

    enum class KeyNotFoundPolicy
    {
        THROW,
        CREATE_EMPTY,
        RETURN_NULL,
    };

    LockedKeyPtr createLockedKey(const Key & key, KeyNotFoundPolicy key_not_found_policy);

    mutable KeysQueuePtr cleanup_keys_metadata_queue;

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

    FileSegments getImpl(
        const Key & key,
        const FileSegment::Range & range,
        const LockedKey & locked_key);

    FileSegments splitRangeInfoFileSegments(
        const Key & key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        const CreateFileSegmentSettings & create_settings,
        LockedKey & locked_key);

    void fillHolesWithEmptyFileSegments(
        FileSegments & file_segments,
        const Key & key,
        const FileSegment::Range & range,
        bool fill_with_detached_file_segments,
        const CreateFileSegmentSettings & settings,
        LockedKey & locked_key);

    KeyMetadata::iterator addFileSegment(
        const Key & key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        const CreateFileSegmentSettings & create_settings,
        LockedKey & locked_key,
        const CacheGuard::Lock *);

    bool tryReserveUnlocked(
        const Key & key,
        size_t offset,
        size_t size,
        LockedKeyPtr locked_key,
        const CacheGuard::Lock &);

    bool tryReserveImpl(
        IFileCachePriority & priority_queue,
        const Key & key,
        size_t offset,
        size_t size,
        LockedKeyPtr locked_key,
        QueryLimit::LockedQueryContext * query_context,
        const CacheGuard::Lock &);

    struct IterateAndLockResult
    {
        IFileCachePriority::IterationResult iteration_result;
        bool lock_key = false;
    };

    using IterateAndCollectLocksFunc = std::function<IterateAndLockResult(const IFileCachePriority::Entry &, LockedKey &)>;
    void iterateAndCollectKeyLocks(
        LockedCachePriority & priority,
        IterateAndCollectLocksFunc && func,
        LockedKeysMap & locked_map) const;

    void performDelayedRemovalOfDeletedKeysFromMetadata(const CacheMetadataGuard::Lock &);
};

}
