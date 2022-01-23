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

#include <base/logger_useful.h>
#include <Common/FileSegment.h>
#include <Core/Types.h>


namespace DB
{

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 */
class FileCache : boost::noncopyable
{
friend class FileSegment;

public:
    using Key = UInt128;

    FileCache(const String & cache_base_path_, size_t max_size_, size_t max_element_size_);

    virtual ~FileCache() = default;

    size_t capacity() const { return max_size; }

    static Key hash(const String & path);

    String path(const Key & key, size_t offset);

    String path(const Key & key);

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

    virtual void remove(const Key & key) = 0;

    /// For debug.
    virtual String dumpStructure() = 0;

protected:
    String cache_base_path;
    size_t max_size = 0;
    size_t max_element_size = 0;

    mutable std::mutex mutex;

    virtual bool tryReserve(
        const Key & key, size_t offset, size_t size,
        [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) = 0;

    virtual void remove(
        Key key, size_t offset,
        [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) = 0;

    virtual bool isLastFileSegmentHolder(
        const Key & key, size_t offset,
        [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) = 0;

    virtual void reduceSizeToDownloaded(
        const Key & key, size_t offset, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) = 0;
};

using FileCachePtr = std::shared_ptr<FileCache>;

class LRUFileCache final : public FileCache
{
public:
    LRUFileCache(const String & cache_base_path_, size_t max_size_, size_t max_element_size_ = 0);

    FileSegmentsHolder getOrSet(const Key & key, size_t offset, size_t size) override;

    void remove(const Key & key) override;

private:
    using FileKeyAndOffset = std::pair<Key, size_t>;
    using LRUQueue = std::list<FileKeyAndOffset>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct FileSegmentCell : boost::noncopyable
    {
        FileSegmentPtr file_segment;

        /// Iterator is put here on first reservation attempt, if successful.
        std::optional<LRUQueueIterator> queue_iterator;

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
    bool startup_restore_finished = false;

    size_t available() const { return max_size - current_size; }

    void restore();

    /**
     * Get list of file segments which intesect with `range`.
     * If `key` is not in cache or there is not such range, return std::nullopt.
     */
    FileSegments getImpl(
        const Key & key, const FileSegment::Range & range,
        [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    FileSegmentCell * getCell(
        const Key & key, size_t offset, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    FileSegmentCell * addCell(
        const Key & key, size_t offset, size_t size,
        FileSegment::State state, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    void removeCell(
        const Key & key, size_t offset, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    void useCell(const FileSegmentCell & cell, FileSegments & result, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    bool tryReserve(
        const Key & key, size_t offset, size_t size,
        [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override;

    void remove(
        Key key, size_t offset,
        [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override;

    bool isLastFileSegmentHolder(
        const Key & key, size_t offset,
        [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override;

    void removeFileKey(const Key & key);

    void reduceSizeToDownloaded(
        const Key & key, size_t offset, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override;

public:
    struct Stat
    {
        size_t size;
        size_t available;
        size_t downloaded_size;
        size_t downloading_size;
    };

    Stat getStat();

    String dumpStructure() override;
};

}
