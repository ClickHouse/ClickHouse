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

#include <Common/logger_useful.h>
#include <Common/FileSegment.h>
#include <Common/IFileCache.h>


namespace DB
{

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 * Implements LRU eviction policy.
 */
class LRUFileCache final : public IFileCache
{
public:
    LRUFileCache(
        const String & cache_base_path_,
        const FileCacheSettings & cache_settings_);

    FileSegmentsHolder getOrSet(const Key & key, size_t offset, size_t size, bool is_persistent) override;

    FileSegmentsHolder get(const Key & key, size_t offset, size_t size) override;

    FileSegments getSnapshot() const override;

    void initialize() override;

    void removeIfExists(const Key & key) override;

    void removeIfReleasable() override;

    std::vector<String> tryGetCachePaths(const Key & key) override;

    size_t getUsedCacheSize() const override;

    size_t getFileSegmentsNum() const override;

private:
    struct FileSegmentCell : private boost::noncopyable
    {
        FileSegmentPtr file_segment;

        /// Iterator is put here on first reservation attempt, if successful.
        std::optional<LRUQueue::Iterator> queue_iterator;

        /// Pointer to file segment is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileSegmentsHolder.
        bool releasable() const {return file_segment.unique(); }

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

    LRUQueue stash_queue;
    AccessRecord records;

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
        FileSegment::State state, bool is_persistent,
        std::lock_guard<std::mutex> & cache_lock);

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

    size_t getAvailableCacheSize() const;

    void loadCacheInfoIntoMemory(std::lock_guard<std::mutex> & cache_lock);

    FileSegments splitRangeIntoCells(
        const Key & key, size_t offset, size_t size, FileSegment::State state, bool is_persistent, std::lock_guard<std::mutex> & cache_lock);

    String dumpStructureUnlocked(const Key & key_, std::lock_guard<std::mutex> & cache_lock);

    void fillHolesWithEmptyFileSegments(
        FileSegments & file_segments,
        const Key & key,
        const FileSegment::Range & range,
        bool fill_with_detached_file_segments,
        bool is_persistent,
        std::lock_guard<std::mutex> & cache_lock);

    FileSegmentPtr createFileSegmentForDownload(
        const Key & key,
        size_t offset,
        size_t size,
        bool is_persistent,
        std::lock_guard<std::mutex> & cache_lock) override;

    size_t getUsedCacheSizeUnlocked(std::lock_guard<std::mutex> & cache_lock) const;

    size_t getAvailableCacheSizeUnlocked(std::lock_guard<std::mutex> & cache_lock) const;

    size_t getFileSegmentsNumUnlocked(std::lock_guard<std::mutex> & cache_lock) const;

    void assertCacheCellsCorrectness(const FileSegmentsByOffset & cells_by_offset, std::lock_guard<std::mutex> & cache_lock);

    void reduceSizeToDownloaded(
        const Key & key, size_t offset,
        std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & /* segment_lock */) override;

public:
    String dumpStructure(const Key & key_) override;

    void assertCacheCorrectness(const Key & key, std::lock_guard<std::mutex> & cache_lock);

    void assertCacheCorrectness(std::lock_guard<std::mutex> & cache_lock);

    void assertQueueCorrectness(std::lock_guard<std::mutex> & cache_lock);
};

}
