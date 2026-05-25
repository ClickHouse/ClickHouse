#include <IO/DiskCacheProvider.h>

#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}


namespace
{
    void appendCacheLogEntry(
        FilesystemCacheLog & cache_log,
        const FileSegment & segment,
        const FileCacheOriginInfo & origin,
        FilesystemCacheLogElement::CacheType cache_type,
        const String & source_file_path,
        ByteRange requested_range)
    {
        const auto seg_range = segment.range();
        FilesystemCacheLogElement elem
        {
            .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
            .query_id = std::string(CurrentThread::getQueryId()),
            .source_file_path = source_file_path,
            .file_segment_range = { seg_range.left, seg_range.right },
            .requested_range = { requested_range.offset, requested_range.end() },
            .cache_type = cache_type,
            .file_segment_key = segment.key().toString(),
            .file_segment_offset = segment.offset(),
            .file_segment_size = seg_range.size(),
            .read_from_cache_attempted = true,
            .read_buffer_id = {},
            .user_id = origin.user_id,
        };
        cache_log.add(std::move(elem));
    }
}


DiskCacheHandle::DiskCacheHandle(
    FileCachePtr cache_,
    FileCacheKey cache_key_,
    FileCacheOriginInfo origin_,
    size_t object_file_offset_,
    size_t object_size_,
    ByteRange requested,
    const FilesystemCacheSettings & cache_settings_,
    std::shared_ptr<FilesystemCacheLog> cache_log_,
    String source_file_path_)
    : cache(std::move(cache_))
    , cache_key(cache_key_)
    , origin(std::move(origin_))
    , object_file_offset(object_file_offset_)
    , object_size(object_size_)
    , cache_settings(cache_settings_)
    , cache_log(std::move(cache_log_))
    , source_file_path(std::move(source_file_path_))
    , requested_range(requested)
{
    /// `FileCache` keys segments by object-local offset (the cache key is
    /// per-object). Convert the caller's file-level `requested` range.
    chassert(requested.offset >= object_file_offset);
    ByteRange requested_in_object{requested.offset - object_file_offset, requested.size};

    /// When `read_from_filesystem_cache_if_exists_otherwise_bypass_cache` is
    /// set (used for background merges/mutations via
    /// `MergeTreeSequentialSource`), only return already-cached segments —
    /// never create new empty segments. Mirrors
    /// `CachedOnDiskReadBufferFromFile::nextFileSegmentsBatch`. Without this,
    /// every merge that reads a not-yet-cached object pollutes the cache,
    /// which `02241_filesystem_cache_on_write_operations` detects.
    ///
    /// `file_segments_limit = 0` (unlimited) — `DiskCacheProvider` is a
    /// one-shot lookup: `status()` / `get()` / `put()` must see every segment
    /// that overlaps `requested`, otherwise miss ranges past the limit are
    /// silently dropped and `ReaderExecutor` returns short data. The
    /// `filesystem_cache_segments_batch_size` setting is designed for the
    /// legacy streaming reader (`CachedOnDiskReadBufferFromFile`) which
    /// fetches segments in pages via `nextFileSegmentsBatch`; we don't have
    /// that loop and the request size is already bounded by the executor's
    /// window (≤ a few segments per call), so disabling the limit here is
    /// both correct and bounded.
    if (cache_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache)
    {
        holder = cache->get(
            cache_key,
            requested_in_object.offset,
            requested_in_object.size,
            /*file_segments_limit=*/0,
            origin.user_id);
    }
    else
    {
        holder = cache->getOrSet(
            cache_key,
            requested_in_object.offset,
            requested_in_object.size,
            object_size,
            CreateFileSegmentSettings{},
            /*file_segments_limit=*/0,
            origin,
            cache_settings.filesystem_cache_boundary_alignment);
    }

    LOG_TRACE(log, "DiskCacheHandle: requested file [{}, {}) = obj [{}, {}), got {} segments",
        requested.offset, requested.end(),
        requested_in_object.offset, requested_in_object.end(),
        holder ? holder->size() : 0);
}

CacheLookupResult DiskCacheHandle::status() const
{
    CacheLookupResult result;
    if (!holder)
        return result;

    /// Translate segments' object-local ranges to file-level for the caller.
    for (const auto & segment : *holder)
    {
        const auto & seg_range = segment->range();
        ByteRange r{seg_range.left + object_file_offset, seg_range.size()};

        auto state = segment->state();
        if (state == FileSegmentState::DOWNLOADED)
        {
            result.hit_ranges.push_back(r);
        }
        else if (state == FileSegmentState::PARTIALLY_DOWNLOADED
              || state == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
        {
            /// A partial segment has a contiguous downloaded prefix
            /// `[seg.left, current_write_offset)` that is safe to read; the
            /// tail still needs to be sourced. Honouring this is what
            /// makes small files (file_size < segment_size) cacheable —
            /// their last segment is necessarily a partial fill.
            size_t cwo_file = segment->getCurrentWriteOffset() + object_file_offset;
            if (cwo_file > r.offset)
                result.hit_ranges.push_back(ByteRange{r.offset, cwo_file - r.offset});
            if (cwo_file < r.end())
                result.miss_ranges.push_back(ByteRange{cwo_file, r.end() - cwo_file});
        }
        else
        {
            result.miss_ranges.push_back(r);
        }
    }
    return result;
}

Rope DiskCacheHandle::get(ByteRange range)
{
    Rope result;
    if (!holder)
        return result;

    /// `range` is file-level; segments are object-local. Translate the
    /// caller's range into object-local for the overlap math, then attach
    /// file-level `logical_offset` to the returned nodes.
    chassert(range.offset >= object_file_offset);
    ByteRange range_in_object{range.offset - object_file_offset, range.size};

    for (const auto & segment : *holder)
    {
        auto state = segment->state();
        if (state != FileSegmentState::DOWNLOADED
            && state != FileSegmentState::PARTIALLY_DOWNLOADED
            && state != FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
            continue;

        const auto & seg_range = segment->range();
        ByteRange seg_r{seg_range.left, seg_range.size()};

        /// For a fully downloaded segment, the readable end is `seg_r.end()`.
        /// For a partial segment, only `[seg_r.offset, current_write_offset)`
        /// is committed — bytes past that point are not on disk yet.
        size_t downloaded_end = (state == FileSegmentState::DOWNLOADED)
            ? seg_r.end()
            : segment->getCurrentWriteOffset();

        if (downloaded_end <= range_in_object.offset || seg_r.offset >= range_in_object.end())
            continue;

        size_t overlap_start = std::max(seg_r.offset, range_in_object.offset);
        size_t overlap_end = std::min(downloaded_end, range_in_object.end());
        if (overlap_end <= overlap_start)
            continue;
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

        /// Node's logical_offset is file-level — translate from object-local.
        result.append(RopeNode{
            std::move(buf), 0, overlap_size, overlap_start + object_file_offset});

        /// Bump per-segment LRU position + hits_count. Mirrors what the legacy
        /// `CachedOnDiskReadBufferFromFile` does on every cache hit; without
        /// it, `system.filesystem_cache.cache_hits` stays at zero and segments
        /// don't move to the protected queue under SLRU.
        segment->increasePriority();

        if (cache_log)
            appendCacheLogEntry(
                *cache_log, *segment, origin,
                FilesystemCacheLogElement::CacheType::READ_FROM_CACHE,
                source_file_path, requested_range);
    }
    return result;
}


size_t DiskCacheHandle::put(ByteRange range, Rope data)
{
    if (!holder)
        return 0;

    /// In bypass mode the ctor used `cache->get` so EMPTY segments don't exist
    /// here, but be explicit: never populate the cache when the caller asked
    /// us to leave it alone.
    if (cache_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache)
        return 0;

    /// `range` and `data`'s nodes are in file-level coordinates;
    /// `FileCache::FileSegment::range()` is object-local. Translate the
    /// caller's range into object-local for the overlap math and shift `data`
    /// into object-local so `Rope::copyTo` sees matching coordinates.
    chassert(range.offset >= object_file_offset);
    ByteRange range_in_object{range.offset - object_file_offset, range.size};
    data.shift(-static_cast<ssize_t>(object_file_offset));

    size_t bytes_written = 0;

    for (auto & segment : *holder)
    {
        auto state = segment->state();

        /// Only write to EMPTY or PARTIALLY_DOWNLOADED segments.
        if (state != FileSegmentState::EMPTY && state != FileSegmentState::PARTIALLY_DOWNLOADED)
            continue;

        const auto & seg_range = segment->range();
        ByteRange seg_r{seg_range.left, seg_range.size()};

        if (seg_r.end() <= range_in_object.offset || seg_r.offset >= range_in_object.end())
            continue;

        /// Try to become the downloader.
        auto downloader_id = segment->getOrSetDownloader();
        if (!segment->isDownloader())
        {
            LOG_TRACE(log, "DiskCacheHandle::put: not downloader for [{}, {}], downloader={}",
                seg_range.left, seg_range.right, downloader_id);
            continue;
        }

        /// `FileSegment::write` is append-only: bytes must be written at
        /// `getCurrentWriteOffset()`. For EMPTY this equals `seg_r.offset`;
        /// for PARTIALLY_DOWNLOADED it is where the previous downloader
        /// stopped. We can only contribute when our data starts at or
        /// before that point — otherwise we would have to leave a hole,
        /// which the segment does not allow.
        size_t write_offset = segment->getCurrentWriteOffset();
        size_t write_end_max = std::min(seg_r.end(), range_in_object.end());

        if (write_offset >= write_end_max || write_offset < range_in_object.offset)
        {
            segment->completePartAndResetDownloader();
            continue;
        }

        /// Find the contiguous prefix of `data` starting at `write_offset`.
        /// If `data` has a gap right at `write_offset`, nothing to do.
        /// If `data` skips a tail (e.g. boundary segment at EOF, or a
        /// hole in the middle of the read window), write only the prefix
        /// and leave the segment PARTIALLY_DOWNLOADED for later continuation.
        ByteRange target{write_offset, write_end_max - write_offset};
        size_t contiguous = target.size;
        auto data_gaps = data.gaps(target);
        if (!data_gaps.empty())
        {
            size_t first_gap_offset = data_gaps.front().offset;
            contiguous = (first_gap_offset > write_offset) ? (first_gap_offset - write_offset) : 0;
        }

        if (contiguous == 0)
        {
            segment->completePartAndResetDownloader();
            continue;
        }

        /// Reserve only the bytes we will actually write — partial fills
        /// are recoverable via `status`/`get` honouring the downloaded
        /// prefix.
        std::string failure_reason;
        bool reserved = segment->reserve(
            contiguous,
            cache_settings.filesystem_cache_reserve_space_wait_lock_timeout_milliseconds,
            failure_reason);

        if (!reserved)
        {
            LOG_TRACE(log, "DiskCacheHandle::put: reserve failed for [{}, {}]: {}",
                seg_range.left, seg_range.right, failure_reason);
            segment->completePartAndResetDownloader();
            continue;
        }

        /// Flatten the contiguous slice of `data` (now in object-local
        /// coords) directly into a buffer for `segment->write()`.
        ///
        /// Hard-check the contiguity invariant before writing: a violation
        /// here would mean we are about to commit uninitialized (zero) bytes
        /// to the cache file. `Rope::copyTo` chasserts the same property,
        /// but chassert is a no-op in release builds — this throw fires
        /// regardless and turns a silent cache-poisoning into a clean
        /// `LOGICAL_ERROR` that the executor can surface.
        ByteRange write_range{write_offset, contiguous};
        if (!data.covers(write_range))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "DiskCacheHandle::put: data does not contiguously cover the range being written: "
                "write_range=[{}, {}), data intervals={}",
                write_range.offset, write_range.end(), data.getIntervals().size());

        std::vector<char, AllocatorWithMemoryTracking<char>> flat_buf(contiguous);
        data.copyTo(flat_buf.data(), write_range);

        segment->write(flat_buf.data(), contiguous, write_offset);

        /// Release downloader role. If we did not reach `seg_r.end()`,
        /// the segment is left PARTIALLY_DOWNLOADED for later continuation.
        segment->completePartAndResetDownloader();
        bytes_written += contiguous;

        LOG_TRACE(log, "DiskCacheHandle::put: wrote {} bytes to [{}, {}] at offset {}",
            contiguous, seg_range.left, seg_range.right, write_offset);

        if (cache_log)
            appendCacheLogEntry(
                *cache_log, *segment, origin,
                FilesystemCacheLogElement::CacheType::READ_FROM_FS_AND_DOWNLOADED_TO_CACHE,
                source_file_path, requested_range);
    }

    return bytes_written;
}


std::unique_ptr<ICacheHandle> DiskCacheProvider::lookup(
    const StoredObject & object,
    size_t object_file_offset,
    ByteRange range_in_file)
{
    auto resolved_key = custom_cache_key.value_or(FileCacheKey::fromPath(object.remote_path));
    auto resolved_origin = custom_origin.value_or(cache->getCommonOriginWithSegmentKeyType(object.local_path));
    return std::make_unique<DiskCacheHandle>(
        cache,
        std::move(resolved_key),
        std::move(resolved_origin),
        object_file_offset,
        object.bytes_size,
        range_in_file,
        cache_settings,
        cache_log,
        object.remote_path);
}

}
