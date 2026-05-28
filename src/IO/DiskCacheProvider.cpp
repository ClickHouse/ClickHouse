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

            /// Translate segment's object-local range to file-level for the
            /// caller. Segments may extend past `requested_range`; the
            /// executor clamps hits to its piece and intentionally keeps
            /// segment-sized misses to let the next cache or the source
            /// populate this cache fully.
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

            cursor = std::max(cursor, seg_range.left + seg_range.size());
        }
    }

    /// Tail gap past the last segment (or the whole request if `holder` was null).
    emit_gap_to(req_obj_end);

    return result;
}

Rope DiskCacheHandle::get(ByteRange range)
{
    Rope result;
    if (!holder)
        return result;

    /// Record the file-level range for the destructor's deferred LRU bump.
    /// Records BEFORE we read so a partial pread failure (which throws) still
    /// leaves a coherent record — though the dtor will simply re-fetch and no-op
    /// for any range whose cached segments are gone by then.
    hits_to_touch.push_back(range);

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

        /// Read from local file via the standard `createReadBufferFromFileBase`
        /// path so cache-hit reads pick up the same accounting the legacy
        /// reader had: `max_local_read_bandwidth` (via
        /// `local_read_settings.local_throttler`), `OpenedFileCache*` /
        /// `ReadBufferFromFileDescriptorRead*` ProfileEvents, and any future
        /// file-read instrumentation. The raw `::open`+`::pread` path skipped
        /// all of this.
        ///
        /// Zero-copy: `buffer_size = 0` in `local_read_settings` means the
        /// reader allocates no internal buffer. We `set(buf->data(), n)` to
        /// rewire `working_buffer` at our `OwnedRopeBuffer` memory, and `next`
        /// calls `pread` directly into it. Same memory shape as the previous
        /// raw `::pread`, with the buffer wrapper around it.
        ///
        /// The segment is pinned by the holder, so the file is guaranteed to
        /// exist for the lifetime of this handle; any failure here is a hard
        /// I/O error (or external tampering), not a race with eviction —
        /// throw rather than silently drop a hit that `status()` already
        /// promised.
        String path = segment->getPath();
        size_t offset_in_file = overlap_start - seg_range.left;

        auto buf = std::make_shared<OwnedRopeBuffer>(overlap_size);

        /// Narrow per-call `ReadSettings`: everything is fixed except the
        /// caller-supplied throttler. `buffer_size = 0` + external-buffer
        /// drive below means the reader allocates no internal buffer and
        /// `pread` writes directly into `buf->data()`. Same pattern as
        /// legacy `CachedOnDiskReadBufferFromFile::getCacheReadBuffer`
        /// (see the long comment there for which fields are deliberately
        /// NOT propagated and why).
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

        /// LRU bump intentionally deferred to `touch()`. `ReaderExecutor`
        /// calls `touch` for every hit AFTER all `put`s complete, so a hit
        /// that sits adjacent to fresh inserts does not become "older" than
        /// the segments just inserted around it (matches the LRU-update
        /// order of a streaming reader; see `02944` for a regression).

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
                source_file_path, requested_range);
    }
    return result;
}

DiskCacheHandle::~DiskCacheHandle()
{
    /// Deferred LRU bump. For each range that was successfully `get`-ed via
    /// this handle, re-fetch the matching segments and call
    /// `increasePriority`. This happens here — at destruction — rather than
    /// inside `get` so the bump lands AFTER any `put` the executor issued
    /// on this handle (or on another handle for the same file). A hit that
    /// sits next to fresh inserts therefore does not become "older" than
    /// the segments just inserted around it.
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

    /// In bypass mode the ctor used `cache->get` so EMPTY segments don't exist
    /// here, but be explicit: never populate the cache when the caller asked
    /// us to leave it alone.
    if (cache_settings.read_if_exists_otherwise_bypass)
        return 0;

    /// `range` and `data`'s nodes are in file-level coordinates;
    /// `FileCache::FileSegment::range()` is object-local. Translate the
    /// caller's range into object-local for the overlap math and shift `data`
    /// into object-local so `Rope::copyTo` sees matching coordinates.
    chassert(range.offset >= object_file_offset);
    ByteRange range_in_object{range.offset - object_file_offset, range.size};
    data.shift(-static_cast<ssize_t>(object_file_offset));

    size_t bytes_written = 0;

    /// Walk the ctor `holder` front-to-back. Segments are stored in
    /// ascending-offset order, and `ReaderExecutor` issues `put` calls in
    /// the same order (one per `status().miss_ranges` element, also sorted).
    /// So the segment we want to write to is always at `holder->front()`.
    ///
    /// `completeAndPopFront` after each processed segment is the load-bearing
    /// detail: without it, the cache holds every segment overlapping the
    /// originally-requested range as "non-releasable", so
    /// `FileCache::reserve` for a later segment can't evict any earlier
    /// segment to make room (`max_size` / `max_elements` would block).
    /// Popping releases the segment from `holder` — the cache is still the
    /// owner via its metadata map; eviction can take it whenever it needs to.
    /// See `02944_dynamically_change_filesystem_cache_size` for the
    /// regression this fixes when 13 segments must fit into a 10-element cache.
    while (!holder->empty())
    {
        auto & segment = holder->front();
        const auto & seg_range = segment.range();

        /// Segment ends before this `put`'s range starts. Either it was
        /// already processed by an earlier `put`, or it's a hit `get()`
        /// has already read from. Either way we are done with it — pop.
        if (seg_range.right + 1 <= range_in_object.offset)
        {
            holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
            continue;
        }

        /// Segment starts at or past this `put`'s range. Leave it in
        /// `holder` for a later `put` (or for the handle's destructor when
        /// no more puts come).
        if (seg_range.left >= range_in_object.end())
            break;

        /// Segment overlaps `range`. Only EMPTY / PARTIALLY_DOWNLOADED accept
        /// writes; everything else (a stray hit in our range, DETACHED, …)
        /// gets popped without writing.
        const auto state = segment.state();
        if (state != FileSegmentState::EMPTY && state != FileSegmentState::PARTIALLY_DOWNLOADED)
        {
            holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
            continue;
        }

        ByteRange seg_r{seg_range.left, seg_range.size()};

        /// Try to become the downloader.
        auto downloader_id = segment.getOrSetDownloader();
        if (!segment.isDownloader())
        {
            LOG_TRACE(log, "DiskCacheHandle::put: not downloader for [{}, {}], downloader={}",
                seg_range.left, seg_range.right, downloader_id);
            holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
            continue;
        }

        /// `FileSegment::write` is append-only: bytes must be written at
        /// `getCurrentWriteOffset()`. For EMPTY this equals `seg_r.offset`;
        /// for PARTIALLY_DOWNLOADED it is where the previous downloader
        /// stopped. We can only contribute when our data starts at or
        /// before that point — otherwise we would have to leave a hole,
        /// which the segment does not allow.
        size_t write_offset = segment.getCurrentWriteOffset();
        size_t write_end_max = std::min(seg_r.end(), range_in_object.end());

        if (write_offset >= write_end_max || write_offset < range_in_object.offset)
        {
            segment.completePartAndResetDownloader();
            holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
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
            segment.completePartAndResetDownloader();
            holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
            continue;
        }

        /// Reserve only the bytes we will actually write — partial fills
        /// are recoverable via `status`/`get` honouring the downloaded
        /// prefix.
        std::string failure_reason;
        bool reserved = segment.reserve(
            contiguous,
            cache_settings.reserve_space_wait_lock_timeout_milliseconds,
            failure_reason);

        if (!reserved)
        {
            LOG_TRACE(log, "DiskCacheHandle::put: reserve failed for [{}, {}]: {}",
                seg_range.left, seg_range.right, failure_reason);
            segment.completePartAndResetDownloader();
            holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
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

        VectorWithMemoryTracking<char> flat_buf(contiguous);
        data.copyTo(flat_buf.data(), write_range);

        /// Mirror `CachedOnDiskReadBufferFromFile::writeCache` error handling.
        /// `FileSegment::write` throws `ErrnoException` on disk failure and
        /// leaves the segment in `PARTIALLY_DOWNLOADED_NO_CONTINUATION`.
        ///   - `ENOSPC` (errno 28) / quota exceeded (errno 122): always fail
        ///     open; the query continues without this segment in the cache.
        ///   - Other I/O errors: respect `cache->skipCacheOnDiskFailure()`;
        ///     if set, log and skip; otherwise rethrow as
        ///     `CACHE_CANNOT_WRITE_TO_CACHE_DISK`. Pre-fix this just threw
        ///     `ErrnoException` and failed the query — even with
        ///     `skip_cache_on_disk_failure = true`.
        bool written_ok = false;
        try
        {
            segment.write(flat_buf.data(), contiguous, write_offset);
            written_ok = true;
        }
        catch (ErrnoException & e)
        {
            const int code = e.getErrno();
            const bool is_no_space_left = code == /* No space left on device */28
                || code == /* Quota exceeded */122;
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
        }

        /// Release downloader role and pop the segment regardless of write
        /// outcome. On failure the segment is left in
        /// `PARTIALLY_DOWNLOADED_NO_CONTINUATION`, so the assertion inside
        /// `completePartAndResetDownloader` (state is DOWNLOADING or
        /// NO_CONTINUATION) still holds. Popping is the load-bearing detail
        /// that keeps the cache able to evict for subsequent reserves —
        /// see the comment on the outer `while (!holder->empty())` loop.
        segment.completePartAndResetDownloader();
        if (written_ok)
        {
            bytes_written += contiguous;
            LOG_TRACE(log, "DiskCacheHandle::put: wrote {} bytes to [{}, {}] at offset {}",
                contiguous, seg_range.left, seg_range.right, write_offset);

            if (cache_log)
                appendCacheLogEntry(
                    *cache_log, segment, origin,
                    FilesystemCacheLogElement::CacheType::READ_FROM_FS_AND_DOWNLOADED_TO_CACHE,
                    source_file_path, requested_range);
        }
        holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
    }

    return bytes_written;
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
    , cache_log(std::move(cache_log_))
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
