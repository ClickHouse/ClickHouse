#pragma once

#include <Core/Types.h>
#include <Common/FileCache_fwd.h>
#include <Common/IFileCachePriority.h>

#include <boost/noncopyable.hpp>
#include <list>
#include <unordered_map>
#include <functional>


namespace DB
{

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentPtr>;
struct FileSegmentsHolder;
struct ReadSettings;

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 */
class IFileCache : private boost::noncopyable
{
friend class FileSegment;
friend struct FileSegmentsHolder;
friend class FileSegmentRangeWriter;

public:
    using Key = IFileCachePriority::Key;

    IFileCache(
        const String & cache_base_path_,
        const FileCacheSettings & cache_settings_);

    virtual ~IFileCache() = default;

    /// Restore cache from local filesystem.
    virtual void initialize() = 0;

    virtual void removeIfExists(const Key & key) = 0;

    virtual void removeIfReleasable(bool remove_persistent_files) = 0;

    static bool isReadOnly();

    /// Cache capacity in bytes.
    size_t capacity() const { return max_size; }

    static Key hash(const String & path);

    String getPathInLocalCache(const Key & key, size_t offset, bool is_persistent) const;

    String getPathInLocalCache(const Key & key) const;

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
    virtual FileSegmentsHolder getOrSet(const Key & key, size_t offset, size_t size, bool is_persistent) = 0;

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

    virtual FileSegmentsHolder setDownloading(const Key & key, size_t offset, size_t size, bool is_persistent) = 0;

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

    virtual void reduceSizeToDownloaded(
        const Key & key, size_t offset,
        std::lock_guard<std::mutex> & cache_lock,
        std::lock_guard<std::mutex> & /* segment_lock */) = 0;

    void assertInitialized() const;

    using AccessKeyAndOffset = std::pair<Key, size_t>;
    struct KeyAndOffsetHash
    {
        std::size_t operator()(const AccessKeyAndOffset & key) const
        {
            return std::hash<UInt128>()(key.first.key) ^ std::hash<UInt64>()(key.second);
        }
    };

    using FileCacheRecords = std::unordered_map<AccessKeyAndOffset, IFileCachePriority::Iterator, KeyAndOffsetHash>;

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
            : max_cache_size(max_cache_size_)
            , skip_download_if_exceeds_query_cache(skip_download_if_exceeds_query_cache_) {}

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
        QueryContextHolder(const String & query_id_, IFileCache * cache_, QueryContextPtr context_);

        QueryContextHolder() = default;

        ~QueryContextHolder();

        String query_id;
        IFileCache * cache = nullptr;
        QueryContextPtr context;
    };

    QueryContextHolder getQueryContextHolder(const String & query_id, const ReadSettings & settings);

};

using FileCachePtr = std::shared_ptr<IFileCache>;

}

namespace std
{
template <> struct hash<DB::IFileCache::Key>
{
    std::size_t operator()(const DB::IFileCache::Key & k) const { return hash<UInt128>()(k.key); }
};

}
