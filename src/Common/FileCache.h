#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <boost/noncopyable.hpp>
#include <map>

#include "FileCache_fwd.h"
#include <base/logger_useful.h>
#include <Common/FileSegment.h>
#include <Core/Types.h>


namespace DB
{

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 */
class IFileCache : private boost::noncopyable
{
friend class FileSegment;

public:
    using Key = UInt128;
    using Downloader = std::unique_ptr<SeekableReadBuffer>;

    IFileCache(
        const String & cache_base_path_,
        size_t max_size_,
        size_t max_element_size_,
        size_t max_file_segment_size_);

    virtual ~IFileCache() = default;

    /// Restore cache from local filesystem.
    virtual void initialize() = 0;

    virtual void remove(const Key & key) = 0;

    static bool shouldBypassCache();

    /// Cache capacity in bytes.
    size_t capacity() const { return max_size; }

    static Key hash(const String & path);

    String getPathInLocalCache(const Key & key, size_t offset);

    String getPathInLocalCache(const Key & key);

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

    /// For debug.
    virtual String dumpStructure(const Key & key) = 0;

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
};

using FileCachePtr = std::shared_ptr<IFileCache>;

class LRUFileCache final : public IFileCache
{
public:
    LRUFileCache(
        const String & cache_base_path_,
        size_t max_size_,
        size_t max_element_size_ = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS,
        size_t max_file_segment_size_ = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE);

    FileSegmentsHolder getOrSet(const Key & key, size_t offset, size_t size) override;

    void initialize() override;

    void remove(const Key & key) override;

private:
    using FileKeyAndOffset = std::pair<Key, size_t>;
    using LRUQueue = std::list<FileKeyAndOffset>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct FileSegmentCell : private boost::noncopyable
    {
        FileSegmentPtr file_segment;

        /// Iterator is put here on first reservation attempt, if successful.
        std::optional<LRUQueueIterator> queue_iterator;

        /// Pointer to file segment is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileSegmentsHolder.
        bool releasable() const { return file_segment.unique(); }

        size_t size() const { return file_segment->reserved_size; }

        FileSegmentCell(FileSegmentPtr file_segment_, LRUQueue & queue_);

        FileSegmentCell(FileSegmentCell && other)
            : file_segment(std::move(other.file_segment))
            , queue_iterator(std::move(other.queue_iterator)) {}

        std::pair<Key, size_t> getKeyAndOffset() const { return std::make_pair(file_segment->key(), file_segment->range().left); }
    };

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;
    using CachedFiles = std::unordered_map<Key, FileSegmentsByOffset>;

    CachedFiles files;
    LRUQueue queue;
    size_t current_size = 0;
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

    size_t availableSize() const { return max_size - current_size; }

    void loadCacheInfoIntoMemory();

    FileSegments splitRangeIntoEmptyCells(
        const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock);

public:
    struct Stat
    {
        size_t size;
        size_t available;
        size_t downloaded_size;
        size_t downloading_size;
    };

    Stat getStat();

    String dumpStructure(const Key & key_) override;
};

}
