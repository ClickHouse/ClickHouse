#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <boost/noncopyable.hpp>

#include <Core/Types.h>
#include <Common/logger_useful.h>
#include <Common/FileSegment.h>
#include "FileCache.h"
#include "FileCacheSettings.h"
#include "FileCache_fwd.h"

namespace DB
{

///
/// ARCFileCache
/// The ARC algorithm implemented according to LRUFileCache, which can effectively
/// avoid the problem of cache pool pollution caused by one-time large-scale cache flushing.
///
class ARCFileCache final : public IFileCache
{
public:
    ARCFileCache(const String & cache_base_path_, const FileCacheSettings & cache_settings_);

    FileSegmentsHolder getOrSet(const Key & key, size_t offset, size_t size) override;

    void initialize() override;

    void remove(const Key & key) override;

    void remove(bool force_remove_unreleasable) override;

    std::vector<String> tryGetCachePaths(const Key & key) override;

    FileSegmentsHolder get(const Key & key, size_t offset, size_t size) override;

    FileSegmentsHolder setDownloading(const Key & key, size_t offset, size_t size) override;

    FileSegments getSnapshot() const override;

private:
    using FileKeyAndOffset = std::pair<Key, size_t>;
    using LRUQueue = std::list<FileKeyAndOffset>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct FileSegmentCell : private boost::noncopyable
    {
        mutable bool is_low = true;

        mutable size_t hit_count = 1;

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

        size_t getQueueSize() const { return lru_queue.size(); }

        size_t getMaxQueueSize() const { return max_element_size; }

        size_t getMaxSpaceBytes() const { return max_size; }

        double getMaxSpaceMegaBytes() const { return 1.0 * max_size / (1024 * 1024); }

        size_t getSpaceBytes() const { return current_size; }

        double getSpaceMegaBytes() const { return 1.0 * current_size / (1024 * 1024); }

        void incrementSpaceBytes(int size) { current_size += size; }

        void incrementMaxSpaceBytes(int size) { max_size += size; }

        void incrementMaxQueueSize(int size) { max_element_size += size; }

        LRUQueue & queue() { return lru_queue; }
    };

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;
    using CachedFiles = std::unordered_map<Key, FileSegmentsByOffset>;

    CachedFiles files;

    size_t max_high_space_size;
    size_t max_high_elem_size;
    size_t move_threshold;

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

    size_t availableSize() const { return max_size - (low_queue.getSpaceBytes() + high_queue.getSpaceBytes()); }

    void loadCacheInfoIntoMemory(std::lock_guard<std::mutex> & cache_lock);

    FileSegments
    splitRangeIntoCells(const Key & key, size_t offset, size_t size, FileSegment::State state, std::lock_guard<std::mutex> & cache_lock);

    bool canMoveCellToHighQueue(const FileSegmentCell & cell);

    bool tryMoveLowToHigh(const FileSegmentCell & cell, std::lock_guard<std::mutex> & cache_lock);

    void fillHolesWithEmptyFileSegments(
        FileSegments & file_segments,
        const Key & key,
        const FileSegment::Range & range,
        bool fill_with_detached_file_segments,
        std::lock_guard<std::mutex> & cache_lock);

public:
    struct Stat
    {
        size_t size;
        size_t available;
        size_t downloaded_size;
        size_t downloading_size;

        size_t low_space_bytes;
        size_t low_queue_size;
        size_t max_low_space_bytes;
        size_t max_low_queue_size;

        size_t high_space_bytes;
        size_t high_queue_size;
        size_t max_high_space_bytes;
        size_t max_high_queue_size;
    };

    Stat getStat();

    String dumpStructure(const Key & key_) override;

    void assertCacheCorrectness(const Key & key, std::lock_guard<std::mutex> & cache_lock);
};

}
