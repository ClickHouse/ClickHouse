#pragma once

#include <Core/Types.h>
#include <Common/FileCache_fwd.h>

#include <boost/noncopyable.hpp>
#include <list>
#include <functional>


namespace DB
{

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentPtr>;
struct FileSegmentsHolder;

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 */
class IFileCache : private boost::noncopyable
{
friend class FileSegment;
friend struct FileSegmentsHolder;
friend class FileSegmentRangeWriter;

public:
    struct Key
    {
        UInt128 key;
        String toString() const;

        Key() = default;
        explicit Key(const UInt128 & key_) : key(key_) {}

        bool operator==(const Key & other) const { return key == other.key; }
    };

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

    String getPathInLocalCache(const Key & key, size_t offset, bool is_persistent);

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

    virtual FileSegmentPtr setDownloading(const Key & key, size_t offset, size_t size, bool is_persistent, std::lock_guard<std::mutex> & cache_lock) = 0;

    void assertInitialized() const;
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
