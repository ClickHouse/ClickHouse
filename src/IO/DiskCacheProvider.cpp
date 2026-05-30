#include <IO/DiskCacheProvider.h>

#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/CurrentThread.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>
#include <chrono>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int CACHE_CANNOT_WRITE_TO_CACHE_DISK;
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
        ByteRange requested_range,
        size_t object_file_offset)
    {
        /// `seg_range` and `source_file_path` are object-local. The handle
        /// keeps `requested_range` in file-level coordinates; translate it
        /// so the whole log row sits in one frame (matches legacy
        /// `CachedOnDiskReadBufferFromFile` per-object logging).
        const size_t requested_local_offset = requested_range.offset >= object_file_offset
            ? requested_range.offset - object_file_offset : 0;
        const auto seg_range = segment.range();
        FilesystemCacheLogElement elem
        {
            .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
            .query_id = std::string(CurrentThread::getQueryId()),
            .source_file_path = source_file_path,
            .file_segment_range = { seg_range.left, seg_range.right },
            .requested_range = { requested_local_offset, requested_local_offset + requested_range.size },
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
    ThrottlerPtr local_throttler_,
    std::shared_ptr<FilesystemCacheLog> cache_log_,
    String source_file_path_)
    : cache(std::move(cache_))
    , cache_key(cache_key_)
    , origin(std::move(origin_))
    , object_file_offset(object_file_offset_)
    , object_size(object_size_)
    , cache_settings(cache_settings_)
    , local_throttler(std::move(local_throttler_))
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
    /// `file_segments_limit = 0` (unlimited): this is a one-shot lookup, so
    /// `status`/`get`/`put` must see every segment overlapping `requested` or
    /// miss ranges past the limit are silently dropped. The batch-size setting
    /// is for the legacy paging reader; the request is already window-bounded.
    if (cache_settings.read_if_exists_otherwise_bypass)
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
            cache_settings.boundary_alignment);
    }

    LOG_TRACE(log, "DiskCacheHandle: requested file [{}, {}) = obj [{}, {}), got {} segments",
        requested.offset, requested.end(),
        requested_in_object.offset, requested_in_object.end(),
        holder ? holder->size() : 0);
}

CacheLookupResult DiskCacheHandle::status() const
{
    CacheLookupResult result;

    /// Walk segments in ascending order and classify hit/miss. Sub-ranges of
    /// `requested_range` not covered by any segment are added to `miss_ranges`
    /// so the executor falls back to the source for them. This matters for
    /// `read_from_filesystem_cache_if_exists_otherwise_bypass_cache=1`: in
    /// that mode the ctor uses `cache->get` (read-only), which returns only
    /// segments that already exist — gaps between them, or a null `holder`,
    /// are common and must surface as misses. Non-bypass mode uses
    /// `cache->getOrSet` which materialises EMPTY segments across the whole
    /// requested range, so the gap-fill is a no-op there but stays as a
    /// defensive invariant (e.g. when the request extends past `object_size`).
    chassert(requested_range.offset >= object_file_offset);
    const size_t req_obj_start = requested_range.offset - object_file_offset;
    const size_t req_obj_end = req_obj_start + requested_range.size;

    size_t cursor = req_obj_start;  /// object-local; first byte not yet classified

    auto emit_gap_to = [&](size_t gap_end_obj)
    {
        size_t clamped = std::min(gap_end_obj, req_obj_end);
        if (clamped > cursor)
            result.miss_ranges.push_back(ByteRange{
                cursor + object_file_offset, clamped - cursor});
        cursor = std::max(cursor, clamped);
    };

    if (holder)
    {
        for (const auto & segment : *holder)
        {
            const auto & seg_range = segment->range();

            /// Pre-segment gap within the requested range → miss.
            emit_gap_to(seg_range.left);

            /// No more segments overlap the request — stop walking; tail
            /// gap is emitted after the loop.
            if (seg_range.left >= req_obj_end)
                break;

            /// Hits are segment-aligned (may extend past `requested_range`);
            /// the executor clamps them to its window. Misses keep their
            /// head at the segment-aligned boundary so the source overread
            /// fills the segment prefix, but the tail is clamped to
            /// `req_obj_end` — fetching past the request would be wasted
            /// I/O the caller didn't ask for.
            ByteRange r{seg_range.left + object_file_offset, seg_range.size()};
            const size_t req_end_file = req_obj_end + object_file_offset;

            auto state = segment->state();
            if (state == FileSegmentState::DOWNLOADED)
            {
                result.hit_ranges.push_back(r);
            }
            else if (state == FileSegmentState::PARTIALLY_DOWNLOADED
                  || state == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION
                  || state == FileSegmentState::DOWNLOADING)
            {
                /// Credit the committed prefix [seg.left, cwo) as a hit and miss
                /// only the tail past `cwo`. For DOWNLOADING this mirrors what
                /// `get` already serves (it reads up to `getCurrentWriteOffset`),
                /// so a concurrent reader of a segment another reader is still
                /// downloading reads the committed prefix from the cache instead
                /// of re-fetching it from the source. `cwo` is a stable lower
                /// bound: the downloader only appends past it.
                size_t cwo_file = segment->getCurrentWriteOffset() + object_file_offset;
                if (cwo_file > r.offset)
                    result.hit_ranges.push_back(ByteRange{r.offset, cwo_file - r.offset});
                const size_t miss_end = std::min(r.end(), req_end_file);
                if (cwo_file < miss_end)
                    result.miss_ranges.push_back(ByteRange{cwo_file, miss_end - cwo_file});
            }
            else
            {
                const size_t miss_end = std::min(r.end(), req_end_file);
                if (r.offset < miss_end)
                    result.miss_ranges.push_back(ByteRange{r.offset, miss_end - r.offset});
            }

            cursor = std::max(cursor, seg_range.left + seg_range.size());
        }
    }

    /// Tail gap past the last segment (or the whole request if `holder` was null).
    emit_gap_to(req_obj_end);

    return result;
}

ICacheHandle::CacheSegmentPin DiskCacheHandle::pinSegmentAt(size_t file_offset) const
{
    /// `file_offset` is file-level; FileCache keys are object-local. Guard the
    /// lower bound — the executor calls this on every miss handle, including
    /// ones for other objects; an offset past this object finds no segment in
    /// the read-only `get` below and returns null.
    if (file_offset < object_file_offset)
        return nullptr;
    const size_t obj_offset = file_offset - object_file_offset;

    /// `put` pops each written segment from this handle's `holder` (so the
    /// cache can evict it for later reserves, see
    /// `02944_dynamically_change_filesystem_cache_size`), so after a window's
    /// put the holder no longer contains the just-filled segment. Re-fetch it
    /// read-only rather than reading the emptied holder.
    auto fresh = cache->get(cache_key, obj_offset, 1, /*file_segments_limit=*/0, origin.user_id);
    if (!fresh || fresh->empty())
        return nullptr;

    /// Take the bare ref BEFORE `fresh` is destroyed: holding it makes us not
    /// the segment's last owner (`isLastOwnerOfFileSegment` == use_count()==2),
    /// so `fresh`'s completion skips the shrink / background-download path and
    /// leaves the segment PARTIALLY_DOWNLOADED (still appendable by the next
    /// window's put). A bare ref — not a FileSegmentsHolder, whose destructor
    /// would re-`complete` the segment — pins it from eviction.
    FileSegmentPtr segment = fresh->getSingleFileSegment();

    const auto state = segment->state();
    const bool partial = state == FileSegmentState::PARTIALLY_DOWNLOADED
                      || state == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION;
    if (!partial || segment->getCurrentWriteOffset() <= segment->range().left)
        return nullptr;

    return std::static_pointer_cast<void>(segment);
}

Rope DiskCacheHandle::get(ByteRange range)
{
    Rope result;
    if (!holder)
        return result;

    /// Record for the dtor's deferred LRU bump before reading, so a throwing
    /// pread still leaves a coherent record (the dtor re-fetches and no-ops for
    /// ranges whose segments are gone).
    hits_to_touch.push_back(range);

    /// File-level `range` → object-local for the overlap math; returned nodes
    /// carry file-level `logical_offset`.
    chassert(range.offset >= object_file_offset);
    ByteRange range_in_object{range.offset - object_file_offset, range.size};

    for (const auto & segment : *holder)
    {
        auto state = segment->state();
        /// `DOWNLOADING` accepted too: another reader may have become the
        /// downloader between `status` and `get` (transition
        /// `PARTIALLY_DOWNLOADED → DOWNLOADING`). The prefix
        /// `[seg.left, getCurrentWriteOffset)` is committed to disk and safe
        /// to read; without this we would silently drop the hit that
        /// `status` already promised.
        if (state != FileSegmentState::DOWNLOADED
            && state != FileSegmentState::PARTIALLY_DOWNLOADED
            && state != FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION
            && state != FileSegmentState::DOWNLOADING)
            continue;

        const auto & seg_range = segment->range();
        ByteRange seg_r{seg_range.left, seg_range.size()};

        /// For a fully downloaded segment, the readable end is `seg_r.end()`.
        /// For partial / downloading segments, only
        /// `[seg_r.offset, current_write_offset)` is committed.
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

        /// Read via the factory so cache-hit reads pick up `local_throttler`
        /// and the file-read ProfileEvents. Zero-copy: `buffer_size = 0` +
        /// `set(buf->data(), n)` below point `working_buffer` at the
        /// `OwnedRopeBuffer` so `pread` lands directly in it. The holder pins
        /// the segment, so any read failure is a hard I/O error, not an
        /// eviction race — throw rather than drop a promised hit.
        String path = segment->getPath();
        size_t offset_in_file = overlap_start - seg_range.left;

        auto buf = std::make_shared<OwnedRopeBuffer>(overlap_size);

        /// `ReadSettings` for the cache-file read are fully fixed except for
        /// the throttler. See `CachedOnDiskReadBufferFromFile::getCacheReadBuffer`
        /// for the canonical explanation of which fields are deliberately
        /// NOT propagated from the caller's settings and why.
        ReadSettings cache_file_read_settings;
        cache_file_read_settings.local_fs_settings.method = LocalFSReadMethod::pread;
        cache_file_read_settings.local_fs_settings.buffer_size = 0;
        cache_file_read_settings.local_throttler = local_throttler;

        auto reader = createReadBufferFromFileBase(
            path, cache_file_read_settings,
            /*read_hint=*/std::nullopt,
            /*file_size=*/std::nullopt,
            segment->getFlagsForLocalRead());

        reader->seek(static_cast<off_t>(offset_in_file), SEEK_SET);

        size_t copied = 0;
        while (copied < overlap_size)
        {
            reader->set(buf->data() + copied, overlap_size - copied);
            if (!reader->next())
                break;  /// EOF — would mean status() promised a hit we can't honor
            const size_t got = reader->available();
            if (got == 0)
                break;
            reader->position() = reader->buffer().end();
            copied += got;
        }

        if (copied != overlap_size)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "DiskCacheHandle::get: short read from cache file {} at offset {}: got {}, expected {}",
                path, offset_in_file, copied, overlap_size);

        /// Node's logical_offset is file-level — translate from object-local.
        result.append(RopeNode{
            std::move(buf), 0, overlap_size, overlap_start + object_file_offset});

        /// Only emit a cache_log entry for fully `DOWNLOADED` segments. For
        /// `PARTIALLY_DOWNLOADED*`, the subsequent `put` that fills the tail
        /// emits a `READ_FROM_FS_AND_DOWNLOADED_TO_CACHE` entry — emitting a
        /// second `READ_FROM_CACHE` here would double-count under the same
        /// `file_segment_range`, which `02242_system_filesystem_cache_log_table`
        /// detects. Legacy `CachedOnDiskReadBufferFromFile` emits one entry
        /// per segment with a single `ReadType`; we mirror that ordering.
        if (cache_log && state == FileSegmentState::DOWNLOADED)
            appendCacheLogEntry(
                *cache_log, *segment, origin,
                FilesystemCacheLogElement::CacheType::READ_FROM_CACHE,
                source_file_path, requested_range, object_file_offset);
    }
    return result;
}

DiskCacheHandle::~DiskCacheHandle()
{
    /// Deferred LRU bump (the contract in `ICacheHandle::put`): re-fetch and
    /// `increasePriority` each `get`-ed range here, at destruction, so the bump
    /// lands after any `put` and a hit next to fresh inserts isn't aged below
    /// them.
    if (hits_to_touch.empty())
        return;

    for (const auto & range : hits_to_touch)
    {
        chassert(range.offset >= object_file_offset);
        const ByteRange range_in_object{range.offset - object_file_offset, range.size};

        /// `cache->get` is read-only — never creates new segments. We use it
        /// here (rather than the ctor's `holder`, which `put` may have
        /// dropped) to find currently-cached segments overlapping `range`.
        /// `file_segments_limit = 0`: every overlap, not the user's batch hint.
        auto touch_holder = cache->get(
            cache_key,
            range_in_object.offset,
            range_in_object.size,
            /*file_segments_limit=*/0,
            origin.user_id);

        if (!touch_holder)
            continue;

        for (const auto & segment : *touch_holder)
        {
            const auto state = segment->state();
            if (state != FileSegmentState::DOWNLOADED
                && state != FileSegmentState::PARTIALLY_DOWNLOADED
                && state != FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
                continue;

            /// `increasePriority` is the canonical hit-bump entry point —
            /// updates `cache_hits` and moves the segment to the protected
            /// queue under SLRU, mirroring `CachedOnDiskReadBufferFromFile`.
            segment->increasePriority();
        }
    }
}


size_t DiskCacheHandle::put(ByteRange range, Rope data)
{
    if (!holder)
        return 0;

    if (cache_settings.read_if_exists_otherwise_bypass)
    {
        if (cache_log)
        {
            chassert(range.offset >= object_file_offset);
            const ByteRange range_in_object{range.offset - object_file_offset, range.size};
            for (const auto & segment : *holder)
            {
                const auto & seg = segment->range();
                if (seg.right + 1 <= range_in_object.offset || seg.left >= range_in_object.end())
                    continue;
                appendCacheLogEntry(
                    *cache_log, *segment, origin,
                    FilesystemCacheLogElement::CacheType::READ_FROM_FS_BYPASSING_CACHE,
                    source_file_path, requested_range, object_file_offset);
            }
        }
        return 0;
    }

    /// `FileSegment::range()` is object-local; translate `range` and shift
    /// `data` so `Rope::copyTo` sees matching coordinates.
    chassert(range.offset >= object_file_offset);
    ByteRange range_in_object{range.offset - object_file_offset, range.size};
    data.shift(-static_cast<ssize_t>(object_file_offset));

    /// Popping after each segment lets the cache evict it for later reserves;
    /// see `02944_dynamically_change_filesystem_cache_size`.
    size_t bytes_written = 0;
    while (!holder->empty())
    {
        auto & segment = holder->front();
        const auto & seg_range = segment.range();

        if (seg_range.right + 1 <= range_in_object.offset)
        {
            holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
            continue;
        }
        if (seg_range.left >= range_in_object.end())
            break;

        bytes_written += writeToSegment(segment, range_in_object, data);
        holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
    }
    return bytes_written;
}

size_t DiskCacheHandle::writeToSegment(FileSegment & segment, ByteRange range_in_object, const Rope & data)
{
    const auto & seg_range = segment.range();

    const auto state = segment.state();
    if (state != FileSegmentState::EMPTY && state != FileSegmentState::PARTIALLY_DOWNLOADED)
        return 0;

    const auto downloader_id = segment.getOrSetDownloader();
    if (!segment.isDownloader())
    {
        LOG_TRACE(log, "DiskCacheHandle::put: not downloader for [{}, {}], downloader={}",
            seg_range.left, seg_range.right, downloader_id);
        return 0;
    }

    /// `FileSegment::write` is append-only — start at `getCurrentWriteOffset`.
    const size_t write_offset = segment.getCurrentWriteOffset();
    const size_t seg_end = seg_range.right + 1;
    const size_t write_end_max = std::min<size_t>(seg_end, range_in_object.end());
    if (write_offset >= write_end_max || write_offset < range_in_object.offset)
    {
        segment.completePartAndResetDownloader();
        return 0;
    }

    /// A gap inside `data` caps the write; the segment stays
    /// PARTIALLY_DOWNLOADED for continuation.
    const ByteRange target{write_offset, write_end_max - write_offset};
    size_t contiguous = target.size;
    if (auto data_gaps = data.gaps(target); !data_gaps.empty())
    {
        const size_t first_gap_offset = data_gaps.front().offset;
        contiguous = (first_gap_offset > write_offset) ? (first_gap_offset - write_offset) : 0;
    }
    if (contiguous == 0)
    {
        segment.completePartAndResetDownloader();
        return 0;
    }

    /// Validate + flatten before `reserve`: `reserve` sets `queue_iterator`
    /// and an exception after that point trips the framework's
    /// `EMPTY ⇒ !queue_iterator` invariant during holder cleanup.
    const ByteRange write_range{write_offset, contiguous};
    if (!data.covers(write_range))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskCacheHandle::put: data does not contiguously cover the range being written: "
            "write_range=[{}, {}), data intervals={}",
            write_range.offset, write_range.end(), data.getIntervals().size());

    VectorWithMemoryTracking<char> flat_buf(contiguous);
    data.copyTo(flat_buf.data(), write_range);

    std::string failure_reason;
    const bool reserved = segment.reserve(
        contiguous,
        cache_settings.reserve_space_wait_lock_timeout_milliseconds,
        failure_reason);
    if (!reserved)
    {
        LOG_TRACE(log, "DiskCacheHandle::put: reserve failed for [{}, {}]: {}",
            seg_range.left, seg_range.right, failure_reason);
        segment.completePartAndResetDownloader();
        return 0;
    }

    const bool written_ok = tryWriteToSegment(segment, flat_buf.data(), contiguous, write_offset);
    segment.completePartAndResetDownloader();

    if (!written_ok)
        return 0;

    LOG_TRACE(log, "DiskCacheHandle::put: wrote {} bytes to [{}, {}] at offset {}",
        contiguous, seg_range.left, seg_range.right, write_offset);
    if (cache_log)
        appendCacheLogEntry(
            *cache_log, segment, origin,
            FilesystemCacheLogElement::CacheType::READ_FROM_FS_AND_DOWNLOADED_TO_CACHE,
            source_file_path, requested_range, object_file_offset);
    return contiguous;
}

bool DiskCacheHandle::tryWriteToSegment(FileSegment & segment, char * data, size_t size, size_t offset)
{
    /// `FileSegment::write` leaves the segment in
    /// `PARTIALLY_DOWNLOADED_NO_CONTINUATION` on `ErrnoException`.
    /// Disk-full / quota are always treated as fail-open; other errors
    /// honour `skipCacheOnDiskFailure`.
    try
    {
        segment.write(data, size, offset);
        return true;
    }
    catch (ErrnoException & e)
    {
        const int code = e.getErrno();
        const bool is_no_space_left = code == 28 || code == 122;
        chassert(segment.state() == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
        if (is_no_space_left)
        {
            LOG_INFO(log, "DiskCacheHandle::put: insert into cache skipped due to insufficient disk space: {}",
                e.displayText());
        }
        else if (cache->skipCacheOnDiskFailure())
        {
            LOG_ERROR(log, "DiskCacheHandle::put: insert into cache skipped due to disk IO error: {}",
                e.displayText());
        }
        else
        {
            throw Exception(ErrorCodes::CACHE_CANNOT_WRITE_TO_CACHE_DISK,
                "Filesystem cache disk IO error (errno {}): {}. "
                "Consider setting skip_cache_on_disk_failure=true in cache config.",
                code, e.displayText());
        }
        return false;
    }
}


DiskCacheProvider::DiskCacheProvider(
    FileCachePtr cache_,
    const FilesystemCacheSettings & cache_settings_,
    const String & query_id_,
    ThrottlerPtr local_throttler_,
    std::shared_ptr<FilesystemCacheLog> cache_log_,
    std::optional<FileCacheKey> custom_cache_key_,
    std::optional<FileCacheOriginInfo> custom_origin_)
    : cache(std::move(cache_))
    , cache_settings(cache_settings_)
    , local_throttler(std::move(local_throttler_))
    , cache_log(cache_settings_.enable_log ? std::move(cache_log_) : nullptr)
    , custom_cache_key(std::move(custom_cache_key_))
    , custom_origin(std::move(custom_origin_))
{
    /// Register a per-query context if `query_id_` is non-empty and the
    /// cache settings request a per-query download budget. `getQueryContextHolder`
    /// returns null when `filesystem_cache_max_download_size == 0` or no query
    /// limit is configured on the cache, which is the unbounded path.
    query_context_holder = cache->getQueryContextHolder(query_id_, cache_settings);
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
        local_throttler,
        cache_log,
        object.remote_path);
}

}
