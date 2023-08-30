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
#include <boost/noncopyable.hpp>

#include <Core/Types.h>
#include <IO/ReadSettings.h>
#include <Common/FileCache_fwd.h>
#include <Common/FileSegment.h>
#include <Common/IFileCachePriority.h>
#include <Common/logger_useful.h>
#include <Common/FileCacheType.h>

namespace DB
{

/// Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
/// Different caching algorithms are implemented based on IFileCachePriority.
class FileCache : private boost::noncopyable
{
    friend class FileSegment;
    friend class IFileCachePriority;
    friend struct FileSegmentsHolder;
    friend class FileSegmentRangeWriter;

public:
    using Key = DB::FileCacheKey;

    FileCache(const String & cache_base_path_, const FileCacheSettings & cache_settings_);

    ~FileCache() = default;

    /// Restore cache from local filesystem.
    void initialize();

    void removeIfExists(const Key & key);

    void removeIfReleasable();

    static bool isReadOnly();

    /// Cache capacity in bytes.
    size_t capacity() const { return max_size; }

    static Key hash(const String & path);

    String getPathInLocalCache(const Key & key, size_t offset, bool is_persistent) const;

    String getPathInLocalCache(const Key & key) const;

    const String & getBasePath() const { return cache_base_path; }

    std::vector<String> tryGetCachePaths(const Key & key);

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
    FileSegmentsHolder getOrSet(const Key & key, size_t offset, size_t size, bool is_persistent);

    /**
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * If file segment has state EMPTY, then it is also marked as "detached". E.g. it is "detached"
     * from cache (not owned by cache), and as a result will never change it's state and will be destructed
     * with the destruction of the holder, while in getOrSet() EMPTY file segments can eventually change
     * it's state (and become DOWNLOADED).
     */
    FileSegmentsHolder get(const Key & key, size_t offset, size_t size);

    /**
     * Create a file segment of exactly requested size with EMPTY state.
     * Throw exception if requested size exceeds max allowed file segment size.
     * This method is for protected usage: file segment range writer uses it
     * to dynamically allocate file segments.
     */
    FileSegmentPtr createFileSegmentForDownload(
         const Key & key,
         size_t offset,
         size_t size,
         bool is_persistent,
         std::lock_guard<std::mutex> & cache_lock);

    FileSegments getSnapshot() const;

    /// For debug.
    String dumpStructure(const Key & key);

    size_t getUsedCacheSize() const;

    size_t getFileSegmentsNum() const;

private:
    String cache_base_path;
    size_t max_size;
    size_t max_element_size;
    size_t max_file_segment_size;
    bool allow_persistent_files;

    bool is_initialized = false;

    mutable std::mutex mutex;

    bool tryReserve(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock);

    void remove(Key key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock);

    bool isLastFileSegmentHolder(
        const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock);

    void reduceSizeToDownloaded(
        const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & /* segment_lock */);

    void assertInitialized() const;

    using AccessKeyAndOffset = std::pair<Key, size_t>;
    struct KeyAndOffsetHash
    {
        std::size_t operator()(const AccessKeyAndOffset & key) const
        {
            return std::hash<UInt128>()(key.first.key) ^ std::hash<UInt64>()(key.second);
        }
    };

    using FileCacheRecords = std::unordered_map<AccessKeyAndOffset, IFileCachePriority::WriteIterator, KeyAndOffsetHash>;

    /// Used to track and control the cache access of each query.
    /// Through it, we can realize the processing of different queries by the cache layer.
    struct QueryContext
    {
        FileCacheRecords records;
        FileCachePriorityPtr priority;

        size_t cache_size = 0;
        size_t max_cache_size;

        bool skip_download_if_exceeds_query_cache;

        QueryContext(size_t max_cache_size_, bool skip_download_if_exceeds_query_cache_)
            : max_cache_size(max_cache_size_), skip_download_if_exceeds_query_cache(skip_download_if_exceeds_query_cache_)
        {
        }

        void remove(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock);

        void reserve(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock);

        void use(const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock);

        size_t getMaxCacheSize() const { return max_cache_size; }

        size_t getCacheSize() const { return cache_size; }

        FileCachePriorityPtr getPriority() { return priority; }

        bool isSkipDownloadIfExceed() const { return skip_download_if_exceeds_query_cache; }
    };

    using QueryContextPtr = std::shared_ptr<QueryContext>;
    using QueryContextMap = std::unordered_map<String, QueryContextPtr>;

    QueryContextMap query_map;

    bool enable_filesystem_query_cache_limit;

    QueryContextPtr getCurrentQueryContext(std::lock_guard<std::mutex> & cache_lock);

    QueryContextPtr getQueryContext(const String & query_id, std::lock_guard<std::mutex> & cache_lock);

    void removeQueryContext(const String & query_id);

    QueryContextPtr getOrSetQueryContext(const String & query_id, const ReadSettings & settings, std::lock_guard<std::mutex> &);

public:
    /// Save a query context information, and adopt different cache policies
    /// for different queries through the context cache layer.
    struct QueryContextHolder : private boost::noncopyable
    {
        QueryContextHolder(const String & query_id_, FileCache * cache_, QueryContextPtr context_);

        QueryContextHolder() = default;

        ~QueryContextHolder();

        String query_id;
        FileCache * cache = nullptr;
        QueryContextPtr context;
    };

    QueryContextHolder getQueryContextHolder(const String & query_id, const ReadSettings & settings);

private:
    struct FileSegmentCell : private boost::noncopyable
    {
        FileSegmentPtr file_segment;

        /// Iterator is put here on first reservation attempt, if successful.
        IFileCachePriority::WriteIterator queue_iterator;

        /// Pointer to file segment is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileSegmentsHolder.
        bool releasable() const { return file_segment.unique(); }

        size_t size() const { return file_segment->reserved_size; }

        FileSegmentCell(FileSegmentPtr file_segment_, FileCache * cache, std::lock_guard<std::mutex> & cache_lock);

        FileSegmentCell(FileSegmentCell && other) noexcept
            : file_segment(std::move(other.file_segment)), queue_iterator(other.queue_iterator)
        {
        }
    };

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;
    using CachedFiles = std::unordered_map<Key, FileSegmentsByOffset>;

    CachedFiles files;
    std::unique_ptr<IFileCachePriority> main_priority;

    FileCacheRecords stash_records;
    std::unique_ptr<IFileCachePriority> stash_priority;

    size_t max_stash_element_size;
    size_t enable_cache_hits_threshold;

    Poco::Logger * log;

    FileSegments getImpl(const Key & key, const FileSegment::Range & range, std::lock_guard<std::mutex> & cache_lock);

    FileSegmentCell * getCell(const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock);

    FileSegmentCell * addCell(
        const Key & key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        bool is_persistent,
        std::lock_guard<std::mutex> & cache_lock);

    void useCell(const FileSegmentCell & cell, FileSegments & result, std::lock_guard<std::mutex> & cache_lock) const;

    bool tryReserveForMainList(
        const Key & key, size_t offset, size_t size, QueryContextPtr query_context, std::lock_guard<std::mutex> & cache_lock);

    size_t getAvailableCacheSize() const;

    void loadCacheInfoIntoMemory(std::lock_guard<std::mutex> & cache_lock);

    FileSegments splitRangeIntoCells(
        const Key & key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        bool is_persistent,
        std::lock_guard<std::mutex> & cache_lock);

    String dumpStructureUnlocked(const Key & key_, std::lock_guard<std::mutex> & cache_lock);

    void fillHolesWithEmptyFileSegments(
        FileSegments & file_segments,
        const Key & key,
        const FileSegment::Range & range,
        bool fill_with_detached_file_segments,
        bool is_persistent,
        std::lock_guard<std::mutex> & cache_lock);

    size_t getUsedCacheSizeUnlocked(std::lock_guard<std::mutex> & cache_lock) const;

    size_t getAvailableCacheSizeUnlocked(std::lock_guard<std::mutex> & cache_lock) const;

    size_t getFileSegmentsNumUnlocked(std::lock_guard<std::mutex> & cache_lock) const;

    void assertCacheCellsCorrectness(const FileSegmentsByOffset & cells_by_offset, std::lock_guard<std::mutex> & cache_lock);

public:
    void assertCacheCorrectness(const Key & key, std::lock_guard<std::mutex> & cache_lock);

    void assertCacheCorrectness(std::lock_guard<std::mutex> & cache_lock);

    void assertPriorityCorrectness(std::lock_guard<std::mutex> & cache_lock);
};

}
