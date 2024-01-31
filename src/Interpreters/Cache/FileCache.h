#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <boost/functional/hash.hpp>

#include <IO/ReadSettings.h>

#include <Common/ThreadPool.h>
#include <Common/StatusFile.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/QueryLimit.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>
#include <Interpreters/Cache/FileCacheSettings.h>
#include <filesystem>


namespace DB
{

/// Track acquired space in cache during reservation
/// to make error messages when no space left more informative.
struct FileCacheReserveStat
{
    struct Stat
    {
        size_t releasable_size = 0;
        size_t releasable_count = 0;

        size_t non_releasable_size = 0;
        size_t non_releasable_count = 0;
    };

    Stat stat;
    std::unordered_map<FileSegmentKind, Stat> stat_by_kind;

    void update(size_t size, FileSegmentKind kind, bool releasable);
};

/// Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
/// Different caching algorithms are implemented using IFileCachePriority.
class FileCache : private boost::noncopyable
{
public:
    using Key = DB::FileCacheKey;
    using QueryLimit = DB::FileCacheQueryLimit;
    using Priority = IFileCachePriority;
    using PriorityEntry = IFileCachePriority::Entry;

    FileCache(const std::string & cache_name, const FileCacheSettings & settings);

    ~FileCache();

    void initialize();

    const String & getBasePath() const;

    static Key createKeyForPath(const String & path);

    String getPathInLocalCache(const Key & key, size_t offset, FileSegmentKind segment_kind) const;

    String getPathInLocalCache(const Key & key) const;

    /**
     * Given an `offset` and `size` representing [offset, offset + size) bytes interval,
     * return list of cached non-overlapping non-empty
     * file segments `[segment1, ..., segmentN]` which intersect with given interval.
     *
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * As long as pointers to returned file segments are held
     * it is guaranteed that these file segments are not removed from cache.
     */
    FileSegmentsHolderPtr getOrSet(
        const Key & key,
        size_t offset,
        size_t size,
        size_t file_size,
        const CreateFileSegmentSettings & settings,
        size_t file_segments_limit = 0);

    /**
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * If file segment has state EMPTY, then it is also marked as "detached". E.g. it is "detached"
     * from cache (not owned by cache), and as a result will never change it's state and will be destructed
     * with the destruction of the holder, while in getOrSet() EMPTY file segments can eventually change
     * it's state (and become DOWNLOADED).
     */
    FileSegmentsHolderPtr get(const Key & key, size_t offset, size_t size, size_t file_segments_limit);

    FileSegmentsHolderPtr set(const Key & key, size_t offset, size_t size, const CreateFileSegmentSettings & settings);

    /// Remove file segment by `key` and `offset`. Throws if file segment does not exist.
    void removeFileSegment(const Key & key, size_t offset);

    /// Remove files by `key`. Throws if key does not exist.
    void removeKey(const Key & key);

    /// Remove files by `key`.
    void removeKeyIfExists(const Key & key);

    /// Removes files by `path`.
    void removePathIfExists(const String & path);

    /// Remove files by `key`.
    void removeAllReleasable();

    std::vector<String> tryGetCachePaths(const Key & key);

    size_t getUsedCacheSize() const;

    size_t getFileSegmentsNum() const;

    size_t getMaxFileSegmentSize() const { return max_file_segment_size; }

    bool tryReserve(FileSegment & file_segment, size_t size, FileCacheReserveStat & stat);

    std::vector<FileSegment::Info> getFileSegmentInfos();

    std::vector<FileSegment::Info> getFileSegmentInfos(const Key & key);

    std::vector<FileSegment::Info> dumpQueue();

    void deactivateBackgroundOperations();

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

    CacheGuard::Lock lockCache() const;

    std::vector<FileSegment::Info> sync();

    using IterateFunc = std::function<void(const FileSegmentInfo &)>;
    void iterate(IterateFunc && func);

    void applySettingsIfPossible(const FileCacheSettings & new_settings, FileCacheSettings & actual_settings);

private:
    using KeyAndOffset = FileCacheKeyAndOffset;

    const size_t max_file_segment_size;
    const size_t bypass_cache_threshold;
    const size_t boundary_alignment;
    size_t load_metadata_threads;

    Poco::Logger * log;

    std::exception_ptr init_exception;
    std::atomic<bool> is_initialized = false;
    mutable std::mutex init_mutex;
    std::unique_ptr<StatusFile> status_file;
    std::atomic<bool> shutdown = false;

    std::mutex apply_settings_mutex;

    CacheMetadata metadata;

    FileCachePriorityPtr main_priority;
    mutable CacheGuard cache_guard;

    struct HitsCountStash
    {
        HitsCountStash(size_t hits_threashold_, size_t queue_size_);
        void clear();

        const size_t hits_threshold;
        const size_t queue_size;

        std::unique_ptr<LRUFileCachePriority> queue;
        using Records = std::unordered_map<KeyAndOffset, Priority::IteratorPtr, FileCacheKeyAndOffsetHash>;
        Records records;
    };

    /**
     * A HitsCountStash allows to cache certain data only after it reached
     * a certain hit rate, e.g. if hit rate it 5, then data is cached on 6th cache hit.
     */
    mutable std::unique_ptr<HitsCountStash> stash;
    /**
     * A QueryLimit allows to control cache write limit per query.
     * E.g. if a query needs n bytes from cache, but it has only k bytes, where 0 <= k <= n
     * then allowed loaded cache size is std::min(n - k, max_query_cache_size).
     */
    FileCacheQueryLimitPtr query_limit;

    void assertInitialized() const;
    void assertCacheCorrectness();

    void loadMetadata();
    void loadMetadataImpl();
    void loadMetadataForKeys(const std::filesystem::path & keys_dir);

    /// Get all file segments from cache which intersect with `range`.
    /// If `file_segments_limit` > 0, return no more than first file_segments_limit
    /// file segments.
    FileSegments getImpl(
        const LockedKey & locked_key,
        const FileSegment::Range & range,
        size_t file_segments_limit) const;

    /// Split range into subranges by max_file_segment_size,
    /// each subrange size must be less or equal to max_file_segment_size.
    std::vector<FileSegment::Range> splitRange(size_t offset, size_t size);

    /// Split range into subranges by max_file_segment_size (same as in splitRange())
    /// and create a new file segment for each subrange.
    /// If `file_segments_limit` > 0, create no more than first file_segments_limit
    /// file segments.
    FileSegments splitRangeIntoFileSegments(
        LockedKey & locked_key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        size_t file_segments_limit,
        const CreateFileSegmentSettings & create_settings);

    void fillHolesWithEmptyFileSegments(
        LockedKey & locked_key,
        FileSegments & file_segments,
        const FileSegment::Range & range,
        size_t file_segments_limit,
        bool fill_with_detached_file_segments,
        const CreateFileSegmentSettings & settings);

    KeyMetadata::iterator addFileSegment(
        LockedKey & locked_key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        const CreateFileSegmentSettings & create_settings,
        const CacheGuard::Lock *);
};

}
