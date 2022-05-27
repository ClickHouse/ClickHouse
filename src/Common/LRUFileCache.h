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

    void removeIfReleasable(bool remove_persistent_files) override;

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
            size_t hits = 0;

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

        void removeAll(std::lock_guard<std::mutex> & cache_lock);

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
        bool releasable() const {return file_segment.unique(); }

        size_t size() const { return file_segment->reserved_size; }

        FileSegmentCell(FileSegmentPtr file_segment_, LRUFileCache * cache, std::lock_guard<std::mutex> & cache_lock);

        FileSegmentCell(FileSegmentCell && other) noexcept
            : file_segment(std::move(other.file_segment))
            , queue_iterator(other.queue_iterator) {}
    };

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;
    using CachedFiles = std::unordered_map<Key, FileSegmentsByOffset>;

    using AccessKeyAndOffset = std::pair<Key, size_t>;

    struct KeyAndOffsetHash
    {
        std::size_t operator()(const AccessKeyAndOffset & key) const
        {
            return std::hash<UInt128>()(key.first) ^ std::hash<UInt64>()(key.second);
        }
    };

    using AccessRecord = std::unordered_map<AccessKeyAndOffset, LRUQueue::Iterator, KeyAndOffsetHash>;

    CachedFiles files;
    LRUQueue queue;

    LRUQueue stash_queue;
    AccessRecord records;
    size_t max_stash_element_size;
    size_t enable_cache_hits_threshold;

    Poco::Logger * log;
    bool allow_remove_persistent_cache_by_default;

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
        FileSegments & file_segments, const Key & key, const FileSegment::Range & range, bool fill_with_detached_file_segments, bool is_persistent, std::lock_guard<std::mutex> & cache_lock);

    FileSegmentPtr setDownloading(const Key & key, size_t offset, size_t size, bool is_persistent, std::lock_guard<std::mutex> & cache_lock) override;

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
