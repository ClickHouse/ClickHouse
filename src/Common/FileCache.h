#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <boost/functional/hash.hpp>
#include <boost/noncopyable.hpp>
#include <map>

#include "FileCache_fwd.h"
#include <IO/ReadSettings.h>
#include <Common/logger_useful.h>
#include <Common/FileSegment.h>
#include <Common/IFileCachePriority.h>
#include <Common/LRUFileCache.h>
#include <Core/Types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class IFileCache;
using FileCachePtr = std::shared_ptr<IFileCache>;

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 */
class IFileCache : private boost::noncopyable
{
friend class FileSegment;
friend struct FileSegmentsHolder;
friend class FileSegmentRangeWriter;

public:
    using Key = UInt128;
    using Downloader = std::unique_ptr<SeekableReadBuffer>;

    IFileCache(
        const String & cache_base_path_,
        const FileCacheSettings & cache_settings_);

    virtual ~IFileCache() = default;

    /// Restore cache from local filesystem.
    virtual void initialize() = 0;

    virtual void remove(const Key & key) = 0;

    virtual void remove() = 0;

    static bool isReadOnly();

    /// Cache capacity in bytes.
    size_t capacity() const { return max_size; }

    static Key hash(const String & path);

    String getPathInLocalCache(const Key & key, size_t offset);

    String getPathInLocalCache(const Key & key);

    const String & getBasePath() const { return cache_base_path; }

    virtual std::vector<String> tryGetCachePaths(const Key & key) = 0;

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
    virtual FileSegmentsHolder getOrSet(const Key & key, size_t offset, size_t size) = 0;

    /**
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * If file segment has state EMPTY, then it is also marked as "detached". E.g. it is "detached"
     * from cache (not owned by cache), and as a result will never change it's state and will be destructed
     * with the destruction of the holder, while in getOrSet() EMPTY file segments can eventually change
     * it's state (and become DOWNLOADED).
     */
    virtual FileSegmentsHolder get(const Key & key, size_t offset, size_t size) = 0;

    virtual FileSegmentsHolder setDownloading(const Key & key, size_t offset, size_t size) = 0;

    virtual FileSegments getSnapshot() const = 0;

    /// For debug.
    virtual String dumpStructure(const Key & key) = 0;

    virtual size_t getUsedCacheSize() const = 0;

    virtual size_t getFileSegmentsNum() const = 0;

protected:
    String cache_base_path;
    size_t max_size;
    size_t max_element_size;
    size_t max_file_segment_size;

    bool is_initialized = false;

    mutable std::mutex mutex;

    virtual bool tryReserve(
        const Key & key, size_t offset, size_t size,
        std::lock_guard<std::mutex> & cache_lock) = 0;

    virtual void remove(
        Key key, size_t offset,
        std::lock_guard<std::mutex> & cache_lock,
        std::lock_guard<std::mutex> & segment_lock) = 0;

    virtual bool isLastFileSegmentHolder(
        const Key & key, size_t offset,
        std::lock_guard<std::mutex> & cache_lock,
        std::lock_guard<std::mutex> & segment_lock) = 0;

    /// If file segment was partially downloaded and then space reservation fails (because of no
    /// space left), then update corresponding cache cell metadata (file segment size).
    virtual void reduceSizeToDownloaded(
        const Key & key, size_t offset,
        std::lock_guard<std::mutex> & cache_lock,
        std::lock_guard<std::mutex> & segment_lock) = 0;

    void assertInitialized() const;

protected:
    using KeyAndOffset = std::pair<Key, size_t>;

    struct KeyAndOffsetHash
    {
        std::size_t operator()(const KeyAndOffset & key) const
        {
            return std::hash<UInt128>()(key.first) ^ std::hash<UInt64>()(key.second);
        }
    };

    using FileCacheRecords = std::unordered_map<KeyAndOffset, IFileCachePriority::Iterator, KeyAndOffsetHash>;

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
            : priority(std::make_shared<LRUFileCache>())
            , max_cache_size(max_cache_size_)
            , skip_download_if_exceeds_query_cache(skip_download_if_exceeds_query_cache_) {}

        void remove(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock)
        {
            if (cache_size < size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Deleted cache size exceeds existing cache size");

            if (!skip_download_if_exceeds_query_cache)
            {
                auto record = records.find({key, offset});
                if (record != records.end())
                {
                    record->second->remove(cache_lock);
                    records.erase({key, offset});
                }
            }
            cache_size -= size;
        }

        void reserve(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock)
        {
            if (cache_size + size > max_cache_size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reserved cache size exceeds the remaining cache size");

            if (!skip_download_if_exceeds_query_cache)
            {
                auto record = records.find({key, offset});
                if (record == records.end())
                {
                    auto queue_iter = priority->add(key, offset, 0, cache_lock);
                    record = records.insert({{key, offset}, queue_iter}).first;
                }
                record->second->incrementSize(size, cache_lock);
            }
            cache_size += size;
        }

        void use(const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock)
        {
            if (!skip_download_if_exceeds_query_cache)
            {
                auto record = records.find({key, offset});
                if (record != records.end())
                    record->second->use(cache_lock);
            }
        }

        size_t getMaxCacheSize() { return max_cache_size; }

        size_t getCacheSize() { return cache_size; }

        FileCachePriorityPtr getPriority() { return priority; }

        bool isSkipDownloadIfExceed() { return skip_download_if_exceeds_query_cache; }
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
        explicit QueryContextHolder(const String & query_id_, IFileCache * cache_, QueryContextPtr context_);

        QueryContextHolder() = default;

        ~QueryContextHolder();

        String query_id {};
        IFileCache * cache = nullptr;
        QueryContextPtr context = nullptr;
    };

    QueryContextHolder getQueryContextHolder(const String & query_id, const ReadSettings & settings);
};

class FileCache final : public IFileCache
{
public:
    FileCache(
        const String & cache_base_path_,
        const FileCacheSettings & cache_settings_);

    FileSegmentsHolder getOrSet(const Key & key, size_t offset, size_t size) override;

    FileSegmentsHolder get(const Key & key, size_t offset, size_t size) override;

    FileSegments getSnapshot() const override;

    void initialize() override;

    void remove(const Key & key) override;

    void remove() override;

    std::vector<String> tryGetCachePaths(const Key & key) override;

    size_t getUsedCacheSize() const override;

    size_t getFileSegmentsNum() const override;

private:
    struct FileSegmentCell : private boost::noncopyable
    {
        FileSegmentPtr file_segment;

        /// Iterator is put here on first reservation attempt, if successful.
        IFileCachePriority::Iterator queue_iterator;

        /// Pointer to file segment is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileSegmentsHolder.
        bool releasable() const { return file_segment.unique(); }

        size_t size() const { return file_segment->reserved_size; }

        FileSegmentCell(FileSegmentPtr file_segment_, FileCache * cache, std::lock_guard<std::mutex> & cache_lock);

        FileSegmentCell(FileSegmentCell && other) noexcept
            : file_segment(std::move(other.file_segment))
            , queue_iterator(other.queue_iterator) {}
    };

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;
    using CachedFiles = std::unordered_map<Key, FileSegmentsByOffset>;

    CachedFiles files;
    FileCachePriorityPtr main_priority;

    FileCacheRecords stash_records;
    FileCachePriorityPtr stash_priority;

    size_t max_stash_element_size;
    size_t enable_cache_hits_threshold;

    Poco::Logger * log;

    FileSegments getImpl(
        const Key & key, const FileSegment::Range & range,
        std::lock_guard<std::mutex> & cache_lock);

    FileSegmentCell * getCell(
        const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock);

    FileSegmentCell * addCell(
        const Key & key, size_t offset, size_t size,
        FileSegment::State state, std::lock_guard<std::mutex> & cache_lock);

    void useCell(const FileSegmentCell & cell, FileSegments & result, std::lock_guard<std::mutex> & cache_lock);

    bool tryReserve(
        const Key & key, size_t offset, size_t size,
        std::lock_guard<std::mutex> & cache_lock) override;

    bool tryReserveForMainList(
        const Key & key, size_t offset, size_t size,
        QueryContextPtr query_context,
        std::lock_guard<std::mutex> & cache_lock);

    void remove(
        Key key, size_t offset,
        std::lock_guard<std::mutex> & cache_lock,
        std::lock_guard<std::mutex> & segment_lock) override;

    bool isLastFileSegmentHolder(
        const Key & key, size_t offset,
        std::lock_guard<std::mutex> & cache_lock,
        std::lock_guard<std::mutex> & segment_lock) override;

    void reduceSizeToDownloaded(
        const Key & key, size_t offset,
        std::lock_guard<std::mutex> & cache_lock,
        std::lock_guard<std::mutex> & segment_lock) override;

    size_t getAvailableCacheSize() const;

    void loadCacheInfoIntoMemory(std::lock_guard<std::mutex> & cache_lock);

    FileSegments splitRangeIntoCells(
        const Key & key, size_t offset, size_t size, FileSegment::State state, std::lock_guard<std::mutex> & cache_lock);

    String dumpStructureUnlocked(const Key & key_, std::lock_guard<std::mutex> & cache_lock);

    void fillHolesWithEmptyFileSegments(
        FileSegments & file_segments, const Key & key, const FileSegment::Range & range, bool fill_with_detached_file_segments, std::lock_guard<std::mutex> & cache_lock);

    FileSegmentsHolder setDownloading(const Key & key, size_t offset, size_t size) override;

    size_t getUsedCacheSizeUnlocked(std::lock_guard<std::mutex> & cache_lock) const;

    size_t getAvailableCacheSizeUnlocked(std::lock_guard<std::mutex> & cache_lock) const;

    size_t getFileSegmentsNumUnlocked(std::lock_guard<std::mutex> & cache_lock) const;

    void assertCacheCellsCorrectness(const FileSegmentsByOffset & cells_by_offset, std::lock_guard<std::mutex> & cache_lock);

public:
    String dumpStructure(const Key & key_) override;

    void assertCacheCorrectness(const Key & key, std::lock_guard<std::mutex> & cache_lock);

    void assertCacheCorrectness(std::lock_guard<std::mutex> & cache_lock);

    void assertPriorityCorrectness(std::lock_guard<std::mutex> & cache_lock);
};

}
