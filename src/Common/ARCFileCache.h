#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <boost/noncopyable.hpp>

#include <Core/Types.h>
#include <base/logger_useful.h>
#include <Common/FileSegment.h>
#include "FileCache.h"
#include "FileCache_fwd.h"

namespace DB
{

/// The ARC algorithm implemented according to LRUFileCache, which can effectively 
/// avoid the problem of cache pool pollution caused by one-time large-scale cache flushing.
class ARCFileCache final : public IFileCache
{
public:
    ARCFileCache(
        const String & cache_base_path_,
        size_t max_size_,
        double size_ratio_ = 0.20,
        int move_threshold_ = 4,
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
        mutable bool is_low = true;

        mutable int hit_count = 0;

        FileSegmentPtr file_segment;

        /// Iterator is put here on first reservation attempt, if successful.
        std::optional<LRUQueueIterator> queue_iterator;

        /// Pointer to file segment is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileSegmentsHolder.
        bool releasable() const { return file_segment.unique(); }

        size_t size() const { return file_segment->reserved_size; }

        double megabytes() const { return 1.0 * file_segment->reserved_size / (1024 * 1024); }

        FileSegmentCell(FileSegmentPtr file_segment_, LRUQueue & queue_);

        FileSegmentCell(FileSegmentCell && other)
            : file_segment(std::move(other.file_segment)), queue_iterator(std::move(other.queue_iterator))
        {
        }

        std::pair<Key, size_t> getKeyAndOffset() const { return std::make_pair(file_segment->key(), file_segment->range().left); }
    };

    struct LRUQueueDescriptor : private boost::noncopyable
    {
        LRUQueue lru_queue;
        size_t max_size;
        size_t max_element_size;
        size_t current_size;

        LRUQueueDescriptor(size_t max_size_, size_t max_element_size_)
            : max_size(max_size_), max_element_size(max_element_size_), current_size(0)
        {
        }

        ~LRUQueueDescriptor() = default;

        size_t queue_size() const { return lru_queue.size(); }

        size_t max_queue_size() const { return max_element_size; }

        size_t max_space_bytes() const { return max_size; }

        double max_space_megabytes() const { return 1.0 * max_size / (1024 * 1024); }

        size_t current_space_bytes() const { return current_size; }

        double current_space_megabytes() const { return 1.0 * current_size / (1024 * 1024); }

        void increase_space_bytes(int size) { current_size += size; }

        void increase_max_space_bytes(int size) { max_size += size; }

        LRUQueue & queue() { return lru_queue; }
    };

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;
    using CachedFiles = std::unordered_map<Key, FileSegmentsByOffset>;

    CachedFiles files;

    double size_ratio;
    size_t min_low_space_size;
    size_t max_high_space_size;
    int move_threshold;

    LRUQueueDescriptor low_queue;
    LRUQueueDescriptor high_queue;

    Poco::Logger * log;

    FileSegments getImpl(const Key & key, const FileSegment::Range & range, std::lock_guard<std::mutex> & cache_lock);

    FileSegmentCell * getCell(const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock);

    FileSegmentCell *
    addCell(const Key & key, size_t offset, size_t size, FileSegment::State state, std::lock_guard<std::mutex> & cache_lock);

    void useCell(const FileSegmentCell & cell, FileSegments & result, std::lock_guard<std::mutex> & cache_lock);

    bool tryReserve(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock) override;

    bool tryReserve(LRUQueueDescriptor & queue, const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock);

    void remove(Key key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock) override;

    bool isLastFileSegmentHolder(
        const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock) override;

    void reduceSizeToDownloaded(
        const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock) override;

    size_t availableSize() const { return max_size - (low_queue.current_space_bytes() + high_queue.current_space_bytes()); }

    void loadCacheInfoIntoMemory();

    FileSegments splitRangeIntoEmptyCells(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock);

    bool canMoveCellToHighQueue(const FileSegmentCell & cell);

    bool tryMoveLowToHigh(const FileSegmentCell & cell, std::lock_guard<std::mutex> & cache_lock);

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
