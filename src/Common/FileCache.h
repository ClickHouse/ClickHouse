#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <boost/noncopyable.hpp>
#include <Core/Types.h>
#include <map>
#include <base/logger_useful.h>

namespace DB
{

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentPtr>;
struct FileSegmentsHolder;

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
     * DOWNLOADING means that either the segment is being downloaded by some other thread or that it
     * is going to be downloaded by the caller (just space reservation happened).
     * EMPTY means that the segment not in cache, not being downloaded and cannot be downloaded
     * by the caller (because of not enough space or max elements limit reached). E.g. returned list is never empty.
     *
     * As long as pointers to returned file segments are hold
     * it is guaranteed that these file segments are not removed from cache.
     *
     * If there is no suitable file segment found in cache, create a cache cell for the whole
     * bytes range [offset, offset + size) as a new file segment and return it with DOWNLOADING state.
     * If there are some intersecting segments (either DOWNLOADED or DOWNLOADING),
     * but not the full range (e.g. there are holes), try reserve space for them.
     * For segments with successfully reserved space - mark their state as DOWNLOADING,
     * for those which cannot possibly be downloaded mark state as EMPTY.
     */
    virtual FileSegmentsHolder getOrSet(const Key & key, size_t offset, size_t size) = 0;

    virtual void remove(const Key & key) = 0;

protected:
    String cache_base_path;
    size_t max_size = 0;
    size_t max_element_size = 0;

    mutable std::mutex mutex;

    virtual void remove(
        const FileSegment & file_segment, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) = 0;
};

using FileCachePtr = std::shared_ptr<FileCache>;

class FileSegment : boost::noncopyable
{
friend class LRUFileCache;

public:
    enum class State
    {
        DOWNLOADED,
        DOWNLOADING,
        ERROR,
        EMPTY,
    };

    FileSegment(size_t offset_, size_t size_, const FileCache::Key & key_, FileCache * cache_, bool empty_ = false)
        : segment_range(offset_, offset_ + size_ - 1)
        , download_state(empty_ ? State::EMPTY : State::DOWNLOADING)
        , downloader(getThreadId()), file_key(key_) , cache(cache_) {}

    /// Represents an interval [left, right] including both boundaries.
    struct Range
    {
        size_t left;
        size_t right;

        Range(size_t left_, size_t right_) : left(left_), right(right_) {}

        size_t size() const { return right - left + 1; }

        String toString() const { return '[' + std::to_string(left) + ',' + std::to_string(right) + ']'; }
    };

    State state() const
    {
        std::lock_guard lock(mutex);
        return download_state;
    }

    void complete(State state);

    State wait();

    void release();

    const Range & range() const { return segment_range; }

    const FileCache::Key & key() const { return file_key; }

    /// State can be DOWNLOADING either if segment is being downloaded by some other thread
    /// or if current thread should download it. This method allows to tell the caller that
    /// he is the one who must do the downloading.
    bool isDownloader() const { return getThreadId() == downloader; }

    static FileSegmentPtr createEmpty(
        size_t offset, size_t size, const FileCache::Key & key, FileCache * cache)
    {
        return std::make_shared<FileSegment>(offset, size, key, cache, true);
    }

private:
    Range segment_range;

    State download_state;
    UInt64 downloader;

    mutable std::mutex mutex;
    std::condition_variable cv;

    /// If end up with ERROR state, need to remove cell from cache. In this case cell is
    /// removed only either by downloader or downloader's by FileSegmentsHolder (in case downloader did not do that).
    FileCache::Key file_key;
    FileCache * cache;
};


struct FileSegmentsHolder : boost::noncopyable
{
    explicit FileSegmentsHolder(FileSegments && file_segments_) : file_segments(file_segments_) {}
    FileSegmentsHolder(FileSegmentsHolder && other) : file_segments(std::move(other.file_segments)) {}

    ~FileSegmentsHolder()
    {
        for (auto & segment : file_segments)
        {
            /// Notify with either DOWNLOADED or ERROR.
            /// In general this must be done manually by downloader by calling segment->complete(state)
            /// for each segment once it has been downloaded or failed to download.
            /// But if not done by downloader, downloader's holder will do that.

            if (segment && segment->isDownloader())
                segment->release();
        }
    }

    FileSegments file_segments;
};


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
        LRUQueueIterator queue_iterator;

        bool releasable() const { return file_segment.unique(); }

        size_t size() const { return file_segment->range().size(); }

        FileSegmentCell(FileSegmentPtr file_segment_, LRUQueueIterator && queue_iterator_)
            : file_segment(file_segment_), queue_iterator(queue_iterator_) {}

        FileSegmentCell(FileSegmentCell && other)
            : file_segment(std::move(other.file_segment))
            , queue_iterator(std::move(other.queue_iterator)) {}
    };

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;
    using CachedFiles = std::unordered_map<Key, FileSegmentsByOffset>;

    CachedFiles files;
    LRUQueue queue;
    size_t current_size = 0;
    Poco::Logger * log;

    /**
     * Get list of file segments which intesect with `range`.
     * If `key` is not in cache or there is not such range, return std::nullopt.
     */
    FileSegments getImpl(
        const Key & key, const FileSegment::Range & range,
        [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    /**
     * Try put file segment of given range in cache. Return nullptr, if unsuccessful.
     */
    FileSegmentCell * setImpl(
        const Key & key, size_t offset, size_t size,
        [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    void remove(const FileSegment & file_segment, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override;

    void removeImpl(const Key & key, size_t offset, const LRUQueueIterator & queue_iterator, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    void removeCell(const Key & key, size_t offset, const LRUQueueIterator & queue_iterator, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    void useCell(const FileSegmentCell & cell, FileSegments & result, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    bool tryReserve(size_t size, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    FileSegmentCell * getCell(const Key & key, size_t offset, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock);

    size_t available() const { return max_size - current_size; }

    void restore();

public:
    struct Stat
    {
        size_t size;
        size_t available;
        size_t downloaded_size;
        size_t downloading_size;
    };

    Stat getStat();
};

}
