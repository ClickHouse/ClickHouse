#include <IO/DiskCacheProvider.h>

#include <Interpreters/FileCache/FileSegment.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_ALL_DATA;
}


DiskCacheHandle::DiskCacheHandle(
    FileCachePtr cache_,
    FileCacheKey cache_key_,
    ByteRange requested,
    size_t file_size_,
    const FilesystemCacheSettings & cache_settings_)
    : cache(std::move(cache_))
    , cache_key(cache_key_)
    , file_size(file_size_)
    , cache_settings(cache_settings_)
{
    /// When `read_from_filesystem_cache_if_exists_otherwise_bypass_cache` is set
    /// (used for background merges/mutations via `MergeTreeSequentialSource`),
    /// only return already-cached segments — never create new empty segments.
    /// Mirrors `CachedOnDiskReadBufferFromFile::nextFileSegmentsBatch`. Without
    /// this, every merge that reads a not-yet-cached object pollutes the cache,
    /// which `02241_filesystem_cache_on_write_operations` detects.
    if (cache_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache)
    {
        holder = cache->get(
            cache_key,
            requested.offset,
            requested.size,
            cache_settings.filesystem_cache_segments_batch_size,
            FileCache::getCommonOrigin().user_id);
    }
    else
    {
        holder = cache->getOrSet(
            cache_key,
            requested.offset,
            requested.size,
            file_size,
            CreateFileSegmentSettings{},
            cache_settings.filesystem_cache_segments_batch_size,
            FileCache::getCommonOrigin(),
            cache_settings.filesystem_cache_boundary_alignment);
    }

    LOG_TRACE(log, "DiskCacheHandle: requested [{}, {}), got {} segments",
        requested.offset, requested.end(),
        holder ? holder->size() : 0);
}

CacheLookupResult DiskCacheHandle::status() const
{
    CacheLookupResult result;
    if (!holder)
        return result;

    for (const auto & segment : *holder)
    {
        const auto & seg_range = segment->range();
        ByteRange r{seg_range.left, seg_range.size()};

        auto state = segment->state();
        if (state == FileSegmentState::DOWNLOADED)
            result.hit_ranges.push_back(r);
        else
            result.miss_ranges.push_back(r);
    }
    return result;
}

Rope DiskCacheHandle::get(ByteRange range)
{
    Rope result;
    if (!holder)
        return result;

    for (const auto & segment : *holder)
    {
        if (segment->state() != FileSegmentState::DOWNLOADED)
            continue;

        const auto & seg_range = segment->range();
        ByteRange seg_r{seg_range.left, seg_range.size()};

        /// Check overlap.
        if (seg_r.end() <= range.offset || seg_r.offset >= range.end())
            continue;

        size_t overlap_start = std::max(seg_r.offset, range.offset);
        size_t overlap_end = std::min(seg_r.end(), range.end());
        size_t overlap_size = overlap_end - overlap_start;

        /// Read from local file. The segment is pinned by the holder, so the
        /// file is guaranteed to exist for the lifetime of this handle; any
        /// failure here is a hard I/O error (or external tampering), not a
        /// race with eviction — throw rather than silently drop a hit that
        /// `status()` already promised.
        String path = segment->getPath();
        size_t offset_in_file = overlap_start - seg_range.left;

        auto buf = std::make_shared<OwnedRopeBuffer>(overlap_size);

        int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
                "DiskCacheHandle::get: open({}) failed: {}", path, errnoToString());

        ssize_t bytes_read = ::pread(fd, buf->data(), overlap_size, offset_in_file);
        int saved_errno = errno;
        if (0 != ::close(fd))
            LOG_WARNING(log, "DiskCacheHandle::get: close failed for {}: {}", path, errnoToString());

        if (bytes_read < 0)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "DiskCacheHandle::get: pread({}, size={}, offset={}) failed: {}",
                path, overlap_size, offset_in_file, errnoToString(saved_errno));

        if (static_cast<size_t>(bytes_read) != overlap_size)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "DiskCacheHandle::get: pread({}) short read: got {} bytes, expected {} at offset {}",
                path, bytes_read, overlap_size, offset_in_file);

        result.append(RopeNode{std::move(buf), 0, overlap_size, overlap_start});

        /// Bump per-segment LRU position + hits_count. Mirrors what the legacy
        /// `CachedOnDiskReadBufferFromFile` does on every cache hit; without it,
        /// `system.filesystem_cache.cache_hits` stays at zero and segments
        /// don't move to the protected queue under SLRU.
        segment->increasePriority();
    }
    return result;
}

bool DiskCacheHandle::put(ByteRange range, Rope data)
{
    if (!holder)
        return false;

    /// In bypass mode the ctor used `cache->get` so EMPTY segments don't exist
    /// here, but be explicit: never populate the cache when the caller asked
    /// us to leave it alone.
    if (cache_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache)
        return false;

    bool any_written = false;

    for (auto & segment : *holder)
    {
        auto state = segment->state();

        /// Only write to EMPTY or PARTIALLY_DOWNLOADED segments.
        if (state != FileSegmentState::EMPTY && state != FileSegmentState::PARTIALLY_DOWNLOADED)
            continue;

        const auto & seg_range = segment->range();
        ByteRange seg_r{seg_range.left, seg_range.size()};

        /// Check overlap.
        if (seg_r.end() <= range.offset || seg_r.offset >= range.end())
            continue;

        /// Try to become the downloader.
        auto downloader_id = segment->getOrSetDownloader();
        if (!segment->isDownloader())
        {
            LOG_TRACE(log, "DiskCacheHandle::put: not downloader for [{}, {}], downloader={}",
                seg_range.left, seg_range.right, downloader_id);
            continue;
        }

        size_t overlap_start = std::max(seg_r.offset, range.offset);
        size_t overlap_end = std::min(seg_r.end(), range.end());
        size_t write_size = overlap_end - overlap_start;

        /// Reserve space.
        std::string failure_reason;
        bool reserved = segment->reserve(
            write_size,
            cache_settings.filesystem_cache_reserve_space_wait_lock_timeout_milliseconds,
            failure_reason);

        if (!reserved)
        {
            LOG_TRACE(log, "DiskCacheHandle::put: reserve failed for [{}, {}]: {}",
                seg_range.left, seg_range.right, failure_reason);
            segment->completePartAndResetDownloader();
            continue;
        }

        /// Extract bytes from the Rope for the overlap region.
        Rope slice = data.slice(ByteRange{overlap_start, write_size});

        /// Flatten the slice into a contiguous buffer for segment->write().
        std::vector<char> flat_buf(write_size);
        size_t pos = 0;
        for (const auto & node : slice.getNodes())
        {
            std::memcpy(flat_buf.data() + pos, node.data(), node.size);
            pos += node.size;
        }

        segment->write(flat_buf.data(), write_size, overlap_start);

        /// Release downloader role. The holder's destructor will finalize.
        segment->completePartAndResetDownloader();
        any_written = true;

        LOG_TRACE(log, "DiskCacheHandle::put: wrote {} bytes to [{}, {}]",
            write_size, seg_range.left, seg_range.right);
    }

    return any_written;
}


std::unique_ptr<ICacheHandle> DiskCacheProvider::lookup(CacheKey key, ByteRange range)
{
    auto cache_key = FileCacheKey::fromPath(key.path);
    return std::make_unique<DiskCacheHandle>(
        cache, cache_key, range, file_size, cache_settings);
}

}
