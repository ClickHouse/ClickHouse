#pragma once
#include <boost/noncopyable.hpp>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>

namespace DB
{
class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;
struct LockedKey;
class LockedCachePriority;


struct FileSegmentMetadata : private boost::noncopyable
{
    FileSegmentPtr file_segment;

    /// Iterator is put here on first reservation attempt, if successful.
    IFileCachePriority::Iterator queue_iterator;

    /// Pointer to file segment is always hold by the cache itself.
    /// Apart from pointer in cache, it can be hold by cache users, when they call
    /// getorSet(), but cache users always hold it via FileSegmentsHolder.
    bool releasable() const { return file_segment.unique(); }

    size_t size() const;

    FileSegmentMetadata(
        FileSegmentPtr file_segment_,
        LockedKey & locked_key,
        LockedCachePriority * locked_queue);

    FileSegmentMetadata(FileSegmentMetadata && other) noexcept
        : file_segment(std::move(other.file_segment)), queue_iterator(std::move(other.queue_iterator)) {}
};


struct KeyMetadata : private std::map<size_t, FileSegmentMetadata>, private boost::noncopyable
{
    friend LockedKey;

public:
    using Map = std::map<size_t, FileSegmentMetadata>;

    using Map::begin;
    using Map::rbegin;
    using Map::end;
    using Map::empty;
    using Map::size;
    using Map::emplace;
    using Map::lower_bound;
    using Map::find;
    using Map::iterator;

    const FileSegmentMetadata * getByOffset(size_t offset) const;
    FileSegmentMetadata * getByOffset(size_t offset);

    const FileSegmentMetadata * tryGetByOffset(size_t offset) const;
    FileSegmentMetadata * tryGetByOffset(size_t offset);

    std::string toString() const;

    KeyGuard::Lock lock() const { return guard.lock(); }

    bool created_base_directory = false;
    bool removed = false;

private:
    mutable KeyGuard guard;
};

using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;


struct CacheMetadata : public std::unordered_map<FileCacheKey, KeyMetadataPtr>, private boost::noncopyable
{
public:
    CacheMetadataGuard::Lock lock() { return guard.lock(); }

private:
    CacheMetadataGuard guard;
};

}
