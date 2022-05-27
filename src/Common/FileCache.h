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
#include <Common/logger_useful.h>
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
};

using FileCachePtr = std::shared_ptr<IFileCache>;

class LRUFileCache final : public IFileCache
{
public:
    LRUFileCache(
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
    class LRUQueue
    {
    public:
        struct FileKeyAndOffset
        {
            Key key;
            size_t offset;
            size_t size;

            FileKeyAndOffset(const Key & key_, size_t offset_, size_t size_) : key(key_), offset(offset_), size(size_) {}
        };

        using Iterator = typename std::list<FileKeyAndOffset>::iterator;

        size_t getTotalWeight(std::lock_guard<std::mutex> & /* cache_lock */) const { return cache_size; }

        size_t getElementsNum(std::lock_guard<std::mutex> & /* cache_lock */) const { return queue.size(); }

        Iterator add(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock);

        void remove(Iterator queue_it, std::lock_guard<std::mutex> & cache_lock);

        void moveToEnd(Iterator queue_it, std::lock_guard<std::mutex> & cache_lock);

        /// Space reservation for a file segment is incremental, so we need to be able to increment size of the queue entry.
        void incrementSize(Iterator queue_it, size_t size_increment, std::lock_guard<std::mutex> & cache_lock);

        void assertCorrectness(LRUFileCache * cache, std::lock_guard<std::mutex> & cache_lock);

        String toString(std::lock_guard<std::mutex> & cache_lock) const;

        bool contains(const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock) const;

        Iterator begin() { return queue.begin(); }

        Iterator end() { return queue.end(); }

    private:
        std::list<FileKeyAndOffset> queue;
        size_t cache_size = 0;
    };

    struct FileSegmentCell : private boost::noncopyable
    {
        FileSegmentPtr file_segment;

        /// Iterator is put here on first reservation attempt, if successful.
        std::optional<LRUQueue::Iterator> queue_iterator;

        /// Pointer to file segment is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileSegmentsHolder.
        bool releasable() const { return file_segment.unique(); }

        size_t size() const { return file_segment->reserved_size; }

        FileSegmentCell(FileSegmentPtr file_segment_, LRUFileCache * cache, std::lock_guard<std::mutex> & cache_lock);

        FileSegmentCell(FileSegmentCell && other) noexcept
            : file_segment(std::move(other.file_segment))
            , queue_iterator(other.queue_iterator) {}
    };

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;
    using CachedFiles = std::unordered_map<Key, FileSegmentsByOffset>;

    CachedFiles files;
    LRUQueue queue;
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
};

}
