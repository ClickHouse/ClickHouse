#include <IO/DiskCacheProvider.h>

#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/FileCache/FileCacheUtils.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/scope_guard_safe.h>
#include <algorithm>
#include <cstring>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int CACHE_CANNOT_WRITE_TO_CACHE_DISK;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
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
    String source_file_path_,
    ReaderAnchorCache * anchors_,
    StreamingReaderSlot * stream_slot_)
    : cache(std::move(cache_))
    , cache_key(cache_key_)
    , origin(std::move(origin_))
    , object_file_offset(object_file_offset_)
    , object_size(object_size_)
    , cache_settings(cache_settings_)
    , local_throttler(std::move(local_throttler_))
    , source_file_path(std::move(source_file_path_))
    , anchors(anchors_)
    , stream_slot(stream_slot_)
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

        /// Reuse the held streaming reader for this segment if it is free, else
        /// open a fresh one (a concurrent `readBigAt`/prefetch shares this
        /// provider, so the slot may be busy or hold another segment). Reuse
        /// avoids re-constructing the reader on every window of a warm stream; a
        /// fresh open is still cheap because `createReadBufferFromFileBase` (pread)
        /// shares the descriptor via `OpenedFileCache`, kept warm by the anchor
        /// cache. `ReadSettings` are fixed except the throttler (see
        /// `CachedOnDiskReadBufferFromFile::getCacheReadBuffer`). The holder pins
        /// the segment, so a read failure is a hard I/O error, not an eviction
        /// race — throw rather than drop a promised hit.
        std::shared_ptr<ReadBufferFromFileBase> reader;
        bool from_slot = false;
        if (stream_slot)
        {
            reader = stream_slot->tryCheckout(path, offset_in_file);
            from_slot = reader != nullptr;
        }
        if (!reader)
        {
            ReadSettings cache_file_read_settings;
            cache_file_read_settings.local_fs_settings.method = LocalFSReadMethod::pread;
            cache_file_read_settings.local_fs_settings.buffer_size = 0;
            cache_file_read_settings.local_throttler = local_throttler;
            reader = createReadBufferFromFileBase(
                path, cache_file_read_settings,
                /*read_hint=*/std::nullopt,
                /*file_size=*/std::nullopt,
                segment->getFlagsForLocalRead());
            /// Position the fresh reader. A reused (from_slot) reader is already
            /// at `offset_in_file` by construction (tryCheckout's contiguity
            /// check) and must NOT be re-`seek`ed — see `StreamingReaderSlot`.
            reader->seek(static_cast<off_t>(offset_in_file), SEEK_SET);
        }

        /// Zero-copy: `buffer_size = 0` + `set(buf->data(), n)` points
        /// `working_buffer` at the `OwnedRopeBuffer` so `pread` lands in it.
        try
        {
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
        }
        catch (...)
        {
            /// Never return a faulted reader to the slot for reuse.
            if (from_slot)
                stream_slot->abandon();
            throw;
        }

        /// Node's logical_offset is file-level — translate from object-local.
        result.append(RopeNode{
            std::move(buf), 0, overlap_size, overlap_start + object_file_offset});

        /// Keep the reader hot for the next window: return/install it as the
        /// streaming reader (sequential reuse) AND anchor it. The anchor keeps up
        /// to 16 segment fds warm for the multi-segment / `readBigAt` case where
        /// the single slot thrashes; the anchor is never read through, race-free.
        if (stream_slot)
            stream_slot->checkin(path, reader, offset_in_file + overlap_size);
        if (anchors)
            anchors->set(path, reader);
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

        /// Best-effort, and this runs from an implicitly-`noexcept` destructor that
        /// is often invoked while unwinding from a read/cache exception. `cache->get`
        /// and `increasePriority` can throw (cache/metadata errors), and the LRU bump
        /// is an optimization, not correctness — so suppress and log per range,
        /// mirroring `FileSegmentsHolder::reset`. A throw escaping here would
        /// `std::terminate` (worse, during unwinding from the original exception).
        try
        {
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
        catch (...)
        {
            tryLogCurrentException(log, "Deferred LRU priority bump failed", LogsLevel::debug);
        }
    }
}


size_t DiskCacheHandle::put(ByteRange range, Rope data)
{
    if (!holder)
        return 0;

    if (cache_settings.read_if_exists_otherwise_bypass)
        return 0;

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
    std::optional<FileCacheKey> custom_cache_key_,
    std::optional<FileCacheOriginInfo> custom_origin_)
    : cache(std::move(cache_))
    , cache_settings(cache_settings_)
    , local_throttler(std::move(local_throttler_))
    , custom_cache_key(std::move(custom_cache_key_))
    , custom_origin(std::move(custom_origin_))
    /// 16 keep-alive anchors, untracked metrics; `EqualWeightFunction` makes the
    /// byte cap an entry count.
    , reader_anchors(CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes=*/16)
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
    return makeHandle(object, object_file_offset, range_in_file, cache_settings);
}

std::unique_ptr<ICacheHandle> DiskCacheProvider::planResidency(
    const StoredObject & object,
    size_t object_file_offset,
    ByteRange range_in_file)
{
    /// `read_if_exists_otherwise_bypass` already gives the exact read-only
    /// handle plan-then-stream needs: the ctor uses `cache->get` (no segment
    /// creation) and `put` returns 0. Flip it on a local copy so the provider's
    /// own settings — and therefore the gap-fill `lookup` path — are untouched.
    auto plan_settings = cache_settings;
    plan_settings.read_if_exists_otherwise_bypass = true;
    return makeHandle(object, object_file_offset, range_in_file, plan_settings);
}

std::unique_ptr<ICacheHandle> DiskCacheProvider::makeHandle(
    const StoredObject & object,
    size_t object_file_offset,
    ByteRange range_in_file,
    const FilesystemCacheSettings & settings)
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
        settings,
        local_throttler,
        object.remote_path,
        &reader_anchors,
        &streaming_slot);
}


namespace
{

/// Shared zero-copy pread of `[overlap_start, overlap_start + overlap_size)`
/// (object-local) out of `segment`, appending a single file-level `RopeNode`
/// (logical offset `overlap_start + object_file_offset`) to `result`. Optionally
/// reuses / refreshes a `StreamingReaderSlot` and anchors the reader. Factored
/// out of `DiskCacheHandle::get` so both buffer kinds share it. The holder pins
/// the segment, so a short read is a hard I/O error — throw, never drop a hit.
void preadSegmentNode(
    Rope & result,
    FileSegment & segment,
    size_t overlap_start,
    size_t overlap_size,
    size_t object_file_offset,
    const ThrottlerPtr & local_throttler,
    ReaderAnchorCache * anchors,
    StreamingReaderSlot * stream_slot)
{
    const String path = segment.getPath();
    const size_t offset_in_file = overlap_start - segment.range().left;

    auto buf = std::make_shared<OwnedRopeBuffer>(overlap_size);

    /// Reuse the held streaming reader for this segment if it is free, else open
    /// a fresh one (pread shares the descriptor via `OpenedFileCache`, kept warm
    /// by the anchor cache). A reused reader is already at `offset_in_file` by
    /// construction (tryCheckout's contiguity check) and must NOT be re-`seek`ed.
    std::shared_ptr<ReadBufferFromFileBase> reader;
    bool from_slot = false;
    if (stream_slot)
    {
        reader = stream_slot->tryCheckout(path, offset_in_file);
        from_slot = reader != nullptr;
    }
    if (!reader)
    {
        ReadSettings cache_file_read_settings;
        cache_file_read_settings.local_fs_settings.method = LocalFSReadMethod::pread;
        cache_file_read_settings.local_fs_settings.buffer_size = 0;
        cache_file_read_settings.local_throttler = local_throttler;
        reader = createReadBufferFromFileBase(
            path, cache_file_read_settings,
            /*read_hint=*/std::nullopt,
            /*file_size=*/std::nullopt,
            segment.getFlagsForLocalRead());
        reader->seek(static_cast<off_t>(offset_in_file), SEEK_SET);
    }

    /// Abandon a checked-out slot reader on ANY exception before check-in — a read
    /// error OR a throw from `result.append` (e.g. bad_alloc). Without this a throw
    /// after checkout leaves the slot permanently `checked_out`, disabling reuse for
    /// the provider's lifetime. Disarmed once `checkin` hands the reader back. A
    /// fresh (non-slot) reader leaves `from_slot` false, so the guard is a no-op.
    SCOPE_EXIT_SAFE({ if (from_slot && stream_slot) stream_slot->abandon(); });

    size_t copied = 0;
    while (copied < overlap_size)
    {
        reader->set(buf->data() + copied, overlap_size - copied);
        if (!reader->next())
            break;
        const size_t got = reader->available();
        if (got == 0)
            break;
        reader->position() = reader->buffer().end();
        copied += got;
    }
    if (copied != overlap_size)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "DiskCacheProvider: short read from cache file {} at offset {}: got {}, expected {}",
            path, offset_in_file, copied, overlap_size);

    result.append(RopeNode{
        std::move(buf), 0, overlap_size, overlap_start + object_file_offset});

    if (stream_slot)
    {
        stream_slot->checkin(path, reader, offset_in_file + overlap_size);
        from_slot = false;  /// disarm: the reader is handed back, no longer checked out
    }
    if (anchors)
        anchors->set(path, reader);
}

}


DiskCacheReadBuffer::DiskCacheReadBuffer(
    std::shared_ptr<FileSegmentsHolder> holder_,
    ByteRange range_in_file,
    size_t object_file_offset_,
    ThrottlerPtr local_throttler_,
    ReaderAnchorCache * anchors_,
    StreamingReaderSlot * stream_slot_,
    std::vector<ByteRange> * hits_to_touch_sink_)
    : holder(std::move(holder_))
    , hit_range(range_in_file)
    , object_file_offset(object_file_offset_)
    , local_throttler(std::move(local_throttler_))
    , anchors(anchors_)
    , stream_slot(stream_slot_)
    , hits_to_touch_sink(hits_to_touch_sink_)
{
}

size_t DiskCacheReadBuffer::readable() const
{
    /// Live committed end of the segment(s) covering `hit_range`, re-read each call
    /// so a concurrent downloader's growth is visible. DOWNLOADED → `hit_range.end()`;
    /// partial / downloading → `cwo + object_file_offset`, clamped to `hit_range.end()`.
    if (!holder)
        return hit_range.offset;

    chassert(hit_range.offset >= object_file_offset);
    const size_t range_obj_off = hit_range.offset - object_file_offset;

    size_t readable_end = hit_range.offset;
    for (const auto & segment : *holder)
    {
        const auto & seg_range = segment->range();
        if (seg_range.right + 1 <= range_obj_off)
            continue;
        if (seg_range.left >= range_obj_off + hit_range.size)
            break;

        const auto state = segment->state();
        size_t committed_end_obj = (state == FileSegmentState::DOWNLOADED)
            ? seg_range.right + 1
            : segment->getCurrentWriteOffset();
        readable_end = std::max(readable_end, committed_end_obj + object_file_offset);
    }

    return std::min(readable_end, hit_range.end());
}

Rope DiskCacheReadBuffer::read(ByteRange sub)
{
    Rope result;
    if (!holder)
        return result;

    /// Clamp to THIS buffer's hit range: every hit buffer of a view shares one
    /// holder spanning all hit segments, so a `read` for a `sub` outside `hit_range`
    /// would serve a neighbouring hit's bytes from the shared holder. The contract is
    /// `sub` within `[range().offset, readable())`; clamp defensively.
    {
        const size_t lo = std::max(sub.offset, hit_range.offset);
        const size_t hi = std::min(sub.end(), hit_range.end());
        if (lo >= hi)
            return result;
        sub = ByteRange{lo, hi - lo};
    }

    /// Record before reading (deferred-bump record): a throwing pread still
    /// leaves a coherent entry that the view's dtor re-fetches and no-ops for
    /// gone segments. Mirrors `DiskCacheHandle::get`.
    if (hits_to_touch_sink)
        hits_to_touch_sink->push_back(sub);

    chassert(sub.offset >= object_file_offset);
    ByteRange sub_in_object{sub.offset - object_file_offset, sub.size};

    for (const auto & segment : *holder)
    {
        const auto state = segment->state();
        if (state != FileSegmentState::DOWNLOADED
            && state != FileSegmentState::PARTIALLY_DOWNLOADED
            && state != FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION
            && state != FileSegmentState::DOWNLOADING)
            continue;

        const auto & seg_range = segment->range();
        const size_t seg_left = seg_range.left;

        const size_t downloaded_end = (state == FileSegmentState::DOWNLOADED)
            ? seg_range.right + 1
            : segment->getCurrentWriteOffset();

        if (downloaded_end <= sub_in_object.offset || seg_left >= sub_in_object.end())
            continue;

        const size_t overlap_start = std::max<size_t>(seg_left, sub_in_object.offset);
        const size_t overlap_end = std::min(downloaded_end, sub_in_object.end());
        if (overlap_end <= overlap_start)
            continue;

        preadSegmentNode(
            result, *segment, overlap_start, overlap_end - overlap_start,
            object_file_offset, local_throttler, anchors, stream_slot);
    }
    return result;
}


DiskCacheWriteBuffer::DiskCacheWriteBuffer(
    FileCachePtr cache_,
    size_t object_file_offset_,
    const FilesystemCacheSettings & cache_settings_,
    FileSegmentsHolderPtr holder_,
    ByteRange aligned_range_in_file)
    : cache(std::move(cache_))
    , object_file_offset(object_file_offset_)
    , cache_settings(cache_settings_)
    , holder(std::move(holder_))
    , aligned_range(aligned_range_in_file)
{
}

bool DiskCacheWriteBuffer::complete() const
{
    /// `committed_ranges` covers the whole aligned range iff subtracting it leaves nothing.
    return committed_ranges.subtract(aligned_range).empty();
}

bool DiskCacheWriteBuffer::tryWriteToSegment(FileSegment & segment, char * data, size_t size, size_t offset)
{
    /// `FileSegment::write` leaves the segment in
    /// `PARTIALLY_DOWNLOADED_NO_CONTINUATION` on `ErrnoException`. Disk-full /
    /// quota are fail-open; other errors honour `skipCacheOnDiskFailure`.
    /// Verbatim copy of `DiskCacheHandle::tryWriteToSegment`.
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
            LOG_INFO(log, "DiskCacheWriteBuffer::write: insert into cache skipped due to insufficient disk space: {}",
                e.displayText());
        }
        else if (cache->skipCacheOnDiskFailure())
        {
            LOG_ERROR(log, "DiskCacheWriteBuffer::write: insert into cache skipped due to disk IO error: {}",
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

size_t DiskCacheWriteBuffer::write(Rope data)
{
    if (cache_settings.read_if_exists_otherwise_bypass)
        return 0;
    if (!holder)
        return 0;

    /// `FileSegment::range()` is object-local; shift `data` so `Rope::copyTo`
    /// sees matching coordinates.
    data.shift(-static_cast<ssize_t>(object_file_offset));

    chassert(aligned_range.offset >= object_file_offset);
    const size_t miss_obj_off = aligned_range.offset - object_file_offset;
    const size_t miss_obj_end = miss_obj_off + aligned_range.size;

    /// Iterate the HELD holder's segments overlapping the still-uncommitted part,
    /// appending append-only at each segment's live `cwo`. Reuses the write math
    /// of `DiskCacheHandle::writeToSegment` but never pops the segment from the
    /// holder (this buffer must keep it appendable across windows). NEVER throws
    /// on the soft skips (detached / wrong state / lost race / no-op).
    size_t bytes_written = 0;
    for (const auto & segment_ptr : *holder)
    {
        FileSegment & segment = *segment_ptr;
        const auto & seg_range = segment.range();

        if (seg_range.right + 1 <= miss_obj_off)
            continue;
        if (seg_range.left >= miss_obj_end)
            break;

        if (segment.isDetached())
            continue;
        const auto st = segment.state();
        if (st != FileSegmentState::EMPTY && st != FileSegmentState::PARTIALLY_DOWNLOADED)
            continue;

        const auto downloader_id = segment.getOrSetDownloader();
        if (!segment.isDownloader())
        {
            LOG_TRACE(log, "DiskCacheWriteBuffer::write: not downloader for [{}, {}], downloader={}",
                seg_range.left, seg_range.right, downloader_id);
            continue;
        }

        /// Release the downloader on ANY exit from this iteration. The normal and
        /// soft-skip paths call `completePartAndResetDownloader` explicitly below
        /// (so this is then a no-op); but the LOGICAL_ERROR throw and any allocation
        /// failure would otherwise leave the segment DOWNLOADING — and since this
        /// buffer is the sole holder, the holder dtor's `complete` would trip
        /// `chassert(!is_last_holder)` / leak the segment. SAFE: swallow on unwind.
        SCOPE_EXIT_SAFE({ if (segment.isDownloader()) segment.completePartAndResetDownloader(); });

        /// Append-only: start at the live `cwo`.
        const size_t write_offset = segment.getCurrentWriteOffset();
        const size_t seg_end = seg_range.right + 1;
        const size_t write_end_max = std::min<size_t>(seg_end, miss_obj_end);
        if (write_offset >= write_end_max || write_offset < miss_obj_off)
        {
            segment.completePartAndResetDownloader();
            continue;
        }

        /// A gap inside `data` caps the write; the segment stays
        /// PARTIALLY_DOWNLOADED for continuation in a later window.
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
            continue;
        }

        /// Validate + flatten before `reserve`: an exception after `reserve`
        /// (which sets `queue_iterator`) trips the framework's
        /// `EMPTY ⇒ !queue_iterator` invariant during holder cleanup.
        const ByteRange write_range{write_offset, contiguous};
        if (!data.covers(write_range))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "DiskCacheWriteBuffer::write: data does not contiguously cover the range being written: "
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
            LOG_TRACE(log, "DiskCacheWriteBuffer::write: reserve failed for [{}, {}]: {}",
                seg_range.left, seg_range.right, failure_reason);
            segment.completePartAndResetDownloader();
            continue;
        }

        const bool written_ok = tryWriteToSegment(segment, flat_buf.data(), contiguous, write_offset);
        /// NEVER completeAndPopFront: keep the segment in our holder, appendable
        /// next window. `completePartAndResetDownloader` releases the downloader
        /// and leaves it PARTIALLY_DOWNLOADED (if bytes landed).
        segment.completePartAndResetDownloader();

        if (!written_ok)
            continue;

        /// File-level committed interval.
        committed_ranges.add(ByteRange{write_offset + object_file_offset, contiguous});
        bytes_written += contiguous;

        LOG_TRACE(log, "DiskCacheWriteBuffer::write: wrote {} bytes to [{}, {}] at offset {}",
            contiguous, seg_range.left, seg_range.right, write_offset);
    }
    return bytes_written;
}

Rope DiskCacheWriteBuffer::read(ByteRange sub)
{
    Rope result;
    if (!holder)
        return result;

    chassert(sub.offset >= object_file_offset);
    ByteRange sub_in_object{sub.offset - object_file_offset, sub.size};

    /// Serve an already-committed prefix from this buffer's own held holder,
    /// downloader-independent (a fresh pread reader, no `StreamingReaderSlot`).
    for (const auto & segment : *holder)
    {
        const auto state = segment->state();
        if (state != FileSegmentState::DOWNLOADED
            && state != FileSegmentState::PARTIALLY_DOWNLOADED
            && state != FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION
            && state != FileSegmentState::DOWNLOADING)
            continue;

        const auto & seg_range = segment->range();
        const size_t seg_left = seg_range.left;
        const size_t downloaded_end = (state == FileSegmentState::DOWNLOADED)
            ? seg_range.right + 1
            : segment->getCurrentWriteOffset();

        if (downloaded_end <= sub_in_object.offset || seg_left >= sub_in_object.end())
            continue;

        const size_t overlap_start = std::max<size_t>(seg_left, sub_in_object.offset);
        const size_t overlap_end = std::min(downloaded_end, sub_in_object.end());
        if (overlap_end <= overlap_start)
            continue;

        preadSegmentNode(
            result, *segment, overlap_start, overlap_end - overlap_start,
            object_file_offset, /*local_throttler=*/nullptr,
            /*anchors=*/nullptr, /*stream_slot=*/nullptr);
    }
    return result;
}

CacheWriteBuffer::CacheSegmentPin DiskCacheWriteBuffer::pin(size_t frontier) const
{
    /// `frontier` is a file-level half-open lower bound. Find the segment in the
    /// held holder containing object-local `(frontier - object_file_offset)` and
    /// return a bare `FileSegmentPtr` into the holder as the pin (keeps it
    /// non-evictable; the holder still owns it for continued appends).
    if (!holder || frontier < object_file_offset)
        return nullptr;
    const size_t frontier_obj = frontier - object_file_offset;

    for (const auto & segment : *holder)
    {
        const auto & seg_range = segment->range();
        if (!seg_range.contains(frontier_obj))
            continue;

        const auto state = segment->state();
        const bool partial = state == FileSegmentState::PARTIALLY_DOWNLOADED
                          || state == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION;
        if (!partial)
            return nullptr;
        if (segment->getCurrentWriteOffset() <= seg_range.left)
            return nullptr;
        if (segment->isDetached())
            return nullptr;

        return std::static_pointer_cast<void>(segment);
    }
    return nullptr;
}


DiskCacheView::DiskCacheView(
    std::shared_ptr<FileSegmentsHolder> read_holder_,
    FileCachePtr cache_,
    FileCacheKey cache_key_,
    FileCacheOriginInfo origin_,
    size_t object_file_offset_)
    : read_holder(std::move(read_holder_))
    , cache(std::move(cache_))
    , cache_key(cache_key_)
    , origin(std::move(origin_))
    , object_file_offset(object_file_offset_)
{
}

DiskCacheView::~DiskCacheView()
{
    /// Deferred LRU bump (the contract in `ICacheHandle::put`): re-fetch and
    /// `increasePriority` each `read`-recorded range, so a hit next to fresh
    /// inserts isn't aged below them. Verbatim from `~DiskCacheHandle`. No write
    /// buffers live in this view, so no finalize-before-bump ordering is needed
    /// here — the executor orders write-buffer destruction before view
    /// destruction in Stage 4.
    if (hits_to_touch.empty())
        return;

    for (const auto & range : hits_to_touch)
    {
        chassert(range.offset >= object_file_offset);
        const ByteRange range_in_object{range.offset - object_file_offset, range.size};

        try
        {
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

                segment->increasePriority();
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Deferred LRU priority bump failed", LogsLevel::debug);
        }
    }
}


CacheViewPtr DiskCacheProvider::planResidencyView(
    const StoredObject & object,
    size_t object_file_offset,
    ByteRange range_in_file)
{
    auto resolved_key = custom_cache_key.value_or(FileCacheKey::fromPath(object.remote_path));
    auto resolved_origin = custom_origin.value_or(cache->getCommonOriginWithSegmentKeyType(object.local_path));

    chassert(range_in_file.offset >= object_file_offset);
    const size_t req_obj_start = range_in_file.offset - object_file_offset;
    /// Clamp the request to the object's end: hits + misses tile only the
    /// in-object portion.
    const size_t req_obj_end = std::min<size_t>(req_obj_start + range_in_file.size, object.bytes_size);

    /// `cache_settings.boundary_alignment` is optional; an unset value means
    /// "use the cache's configured alignment" — exactly how `getOrSet` resolves
    /// it. Resolve here so the miss-range alignment math below matches what
    /// `openWriteBuffers`' `getOrSet` will produce.
    const size_t boundary_alignment = cache_settings.boundary_alignment.value_or(cache->getBoundaryAlignment());
    const size_t object_size = object.bytes_size;

    /// Read-only residency probe — never creates segments (so a fully-resident
    /// range costs nothing beyond the probe and a missed range stays empty).
    auto read_holder = std::make_shared<FileSegmentsHolder>();
    if (req_obj_end > req_obj_start)
    {
        auto got = cache->get(
            resolved_key,
            req_obj_start,
            req_obj_end - req_obj_start,
            /*file_segments_limit=*/0,
            resolved_origin.user_id);
        if (got)
            read_holder = std::shared_ptr<FileSegmentsHolder>(std::move(got));
    }

    auto view = std::make_unique<DiskCacheView>(
        read_holder, cache, resolved_key, resolved_origin, object_file_offset);

    /// Collect raw (unaligned) object-local miss sub-ranges as we classify; the
    /// cache-alignment + merge happens in a second pass so adjacent misses fold.
    std::vector<ByteRange> raw_miss_obj;
    auto add_miss_obj = [&](size_t off, size_t end)
    {
        const size_t clamped_end = std::min(end, req_obj_end);
        if (clamped_end > off)
            raw_miss_obj.push_back(ByteRange{off, clamped_end - off});
    };

    auto add_hit = [&](size_t off_obj, size_t end_obj)
    {
        const size_t clamped_end = std::min(end_obj, req_obj_end);
        if (clamped_end <= off_obj)
            return;
        const ByteRange hit_file{off_obj + object_file_offset, clamped_end - off_obj};
        auto reader = std::make_unique<DiskCacheReadBuffer>(
            read_holder, hit_file, object_file_offset,
            local_throttler, &reader_anchors, &streaming_slot, &view->hits_to_touch);
        view->hit_entries.push_back(HitEntry{hit_file, std::move(reader)});
    };

    /// Walk segments in ascending order, mirroring `DiskCacheHandle::status`'s
    /// prefix/tail gap logic. `get` returns a contiguous tiling padded with
    /// EMPTY/DETACHED placeholders for gaps, but stay defensive about gaps.
    size_t cursor = req_obj_start;
    for (const auto & segment : *read_holder)
    {
        const auto & seg_range = segment->range();

        /// Pre-segment gap within the request → miss.
        if (seg_range.left > cursor)
            add_miss_obj(cursor, seg_range.left);

        if (seg_range.left >= req_obj_end)
        {
            cursor = std::max(cursor, req_obj_end);
            break;
        }

        const size_t seg_left = std::max<size_t>(seg_range.left, req_obj_start);
        const size_t seg_end = seg_range.right + 1;

        const auto state = segment->state();
        if (state == FileSegmentState::DOWNLOADED)
        {
            add_hit(seg_left, seg_end);
        }
        else if (state == FileSegmentState::PARTIALLY_DOWNLOADED
              || state == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION
              || state == FileSegmentState::DOWNLOADING)
        {
            const size_t cwo = segment->getCurrentWriteOffset();
            if (cwo > seg_left)
                add_hit(seg_left, cwo);
            const size_t miss_off = std::max(cwo, seg_left);
            if (miss_off < seg_end)
                add_miss_obj(miss_off, seg_end);
        }
        else
        {
            /// EMPTY / DETACHED gap placeholder from `get`'s fill — miss.
            add_miss_obj(seg_left, seg_end);
        }

        cursor = std::max(cursor, seg_end);
    }

    /// Tail gap past the last segment (or the whole request if the holder is empty).
    if (cursor < req_obj_end)
        add_miss_obj(cursor, req_obj_end);

    /// Align each raw miss to the cache boundary (clamped to the object end),
    /// sort, then merge adjacent/overlapping aligned ranges so they fold
    /// (alignment folded IN, not a separate `alignToCaches`). Emit file-level
    /// `MissEntry{aligned, /*writer=*/nullptr}` — `planResidencyView` never opens
    /// writers.
    std::vector<ByteRange> aligned_vec;
    aligned_vec.reserve(raw_miss_obj.size());
    for (const auto & m : raw_miss_obj)
    {
        const size_t a_off = FileCacheUtils::roundDownToMultiple(m.offset, boundary_alignment);
        size_t a_end = FileCacheUtils::roundUpToMultiple(m.end(), boundary_alignment);
        a_end = std::min(a_end, object_size);
        if (a_end > a_off)
            aligned_vec.push_back(ByteRange{a_off, a_end - a_off});
    }
    std::sort(aligned_vec.begin(), aligned_vec.end(),
        [](const ByteRange & l, const ByteRange & r) { return l.offset < r.offset; });
    /// Merge adjacent/overlapping aligned ranges in OBJECT-LOCAL space, then
    /// translate to file-level when emitting. Merging directly against the
    /// file-level `miss_entries` offsets would mix coordinate spaces and corrupt
    /// the merge whenever `object_file_offset > 0`.
    std::vector<ByteRange> merged_obj;
    for (const auto & a : aligned_vec)
    {
        if (!merged_obj.empty() && a.offset <= merged_obj.back().end())
        {
            auto & last = merged_obj.back();
            last.size = std::max(last.end(), a.end()) - last.offset;
        }
        else
            merged_obj.push_back(a);
    }
    for (const auto & m : merged_obj)
        view->miss_entries.push_back(
            MissEntry{ByteRange{m.offset + object_file_offset, m.size}, /*writer=*/nullptr});

    /// Hits are emitted in ascending order already; keep both sorted by offset.
    std::sort(view->hit_entries.begin(), view->hit_entries.end(),
        [](const HitEntry & l, const HitEntry & r) { return l.range.offset < r.range.offset; });

    LOG_TRACE(log, "planResidencyView: file [{}, {}) → {} hits, {} misses",
        range_in_file.offset, range_in_file.end(), view->hit_entries.size(), view->miss_entries.size());

    return view;
}

std::vector<MissEntry> DiskCacheProvider::openWriteBuffers(
    const StoredObject & object,
    size_t object_file_offset,
    const std::vector<ByteRange> & aligned_miss_ranges)
{
    if (!populatesOnMiss())
        return {};

    auto resolved_key = custom_cache_key.value_or(FileCacheKey::fromPath(object.remote_path));
    auto resolved_origin = custom_origin.value_or(cache->getCommonOriginWithSegmentKeyType(object.local_path));

    std::vector<MissEntry> result;
    result.reserve(aligned_miss_ranges.size());

    for (const auto & aligned_file : aligned_miss_ranges)
    {
        chassert(aligned_file.offset >= object_file_offset);
        const size_t obj_offset = aligned_file.offset - object_file_offset;

        auto holder = cache->getOrSet(
            resolved_key,
            obj_offset,
            aligned_file.size,
            object.bytes_size,
            CreateFileSegmentSettings{},
            /*file_segments_limit=*/0,
            resolved_origin,
            cache_settings.boundary_alignment);

        auto writer = std::make_unique<DiskCacheWriteBuffer>(
            cache,
            object_file_offset,
            cache_settings,
            std::move(holder),
            aligned_file);

        result.push_back(MissEntry{aligned_file, std::move(writer)});
    }

    return result;
}

}
