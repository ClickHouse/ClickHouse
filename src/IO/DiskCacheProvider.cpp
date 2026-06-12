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


namespace
{

/// Shared zero-copy pread of `[overlap_start, overlap_start + overlap_size)`
/// (object-local) out of `segment`, appending a single file-level `RopeNode`
/// (logical offset `overlap_start + object_file_offset`) to `result`. Optionally
/// reuses / refreshes a `StreamingReaderSlot` and anchors the reader. Shared by
/// the read buffer and the write buffer's served-prefix read. The holder pins
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


DiskCacheReader::DiskCacheReader(
    std::shared_ptr<FileSegmentsHolder> holder_,
    ByteRange range_in_file,
    size_t object_file_offset_,
    ThrottlerPtr local_throttler_,
    ReaderAnchorCache * anchors_,
    StreamingReaderSlot * stream_slot_,
    VectorWithMemoryTracking<ByteRange> * hits_to_touch_sink_)
    : holder(std::move(holder_))
    , hit_range(range_in_file)
    , object_file_offset(object_file_offset_)
    , local_throttler(std::move(local_throttler_))
    , anchors(anchors_)
    , stream_slot(stream_slot_)
    , hits_to_touch_sink(hits_to_touch_sink_)
{
}

size_t DiskCacheReader::readable() const
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

Rope DiskCacheReader::read(ByteRange sub)
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
    /// gone segments.
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


DiskCacheWriter::DiskCacheWriter(
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

bool DiskCacheWriter::complete() const
{
    /// `committed_ranges` covers the whole aligned range iff subtracting it leaves nothing.
    return committed_ranges.subtract(aligned_range).empty();
}

bool DiskCacheWriter::tryWriteToSegment(FileSegment & segment, char * data, size_t size, size_t offset)
{
    /// `FileSegment::write` leaves the segment in
    /// `PARTIALLY_DOWNLOADED_NO_CONTINUATION` on `ErrnoException`. Disk-full /
    /// quota are fail-open; other errors honour `skipCacheOnDiskFailure`.
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
            LOG_INFO(log, "DiskCacheWriter::write: insert into cache skipped due to insufficient disk space: {}",
                e.displayText());
        }
        else if (cache->skipCacheOnDiskFailure())
        {
            LOG_ERROR(log, "DiskCacheWriter::write: insert into cache skipped due to disk IO error: {}",
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

size_t DiskCacheWriter::write(Rope data)
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
    /// appending append-only at each segment's live `cwo`, but never popping the
    /// segment from the holder (this buffer must keep it appendable across
    /// windows). NEVER throws on the soft skips (detached / wrong state / lost
    /// race / no-op).
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
            LOG_TRACE(log, "DiskCacheWriter::write: not downloader for [{}, {}], downloader={}",
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
                "DiskCacheWriter::write: data does not contiguously cover the range being written: "
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
            LOG_TRACE(log, "DiskCacheWriter::write: reserve failed for [{}, {}]: {}",
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

        LOG_TRACE(log, "DiskCacheWriter::write: wrote {} bytes to [{}, {}] at offset {}",
            contiguous, seg_range.left, seg_range.right, write_offset);
    }
    return bytes_written;
}

Rope DiskCacheWriter::read(ByteRange sub)
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

CacheWriter::CacheSegmentPin DiskCacheWriter::pin(size_t frontier) const
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
    /// Deferred LRU bump: re-fetch and `increasePriority` each `read`-recorded
    /// range, so a hit next to fresh inserts isn't aged below them. No write
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
    VectorWithMemoryTracking<ByteRange> raw_miss_obj;
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
        auto reader = std::make_unique<DiskCacheReader>(
            read_holder, hit_file, object_file_offset,
            local_throttler, &reader_anchors, &streaming_slot, &view->hits_to_touch);
        view->hit_entries.push_back(HitEntry{hit_file, std::move(reader)});
    };

    /// Walk segments in ascending order with prefix/tail gap logic. `cache->get`
    /// returns a contiguous tiling padded with EMPTY/DETACHED placeholders for
    /// gaps, but stay defensive about gaps.
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
    VectorWithMemoryTracking<ByteRange> aligned_vec;
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
    VectorWithMemoryTracking<ByteRange> merged_obj;
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

VectorWithMemoryTracking<MissEntry> DiskCacheProvider::openWriteBuffers(
    const StoredObject & object,
    size_t object_file_offset,
    const VectorWithMemoryTracking<ByteRange> & aligned_miss_ranges)
{
    if (!populatesOnMiss())
        return {};

    auto resolved_key = custom_cache_key.value_or(FileCacheKey::fromPath(object.remote_path));
    auto resolved_origin = custom_origin.value_or(cache->getCommonOriginWithSegmentKeyType(object.local_path));

    VectorWithMemoryTracking<MissEntry> result;
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

        auto writer = std::make_unique<DiskCacheWriter>(
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
