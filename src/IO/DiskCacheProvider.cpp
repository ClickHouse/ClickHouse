#include <IO/DiskCacheProvider.h>

#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>
#include <algorithm>
#include <cstring>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int CACHE_CANNOT_WRITE_TO_CACHE_DISK;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Shared zero-copy pread of `[overlap_start, overlap_start + overlap_size)`
/// (object-local) out of `segment`, appending a single file-level `ChainedBufferNode`
/// (logical offset `overlap_start + object_file_offset`) to `result`. Opens a fresh
/// pread reader each call - the descriptor is shared via `OpenedFileCache`, kept warm by
/// the anchor cache, so the open is typically syscall-free. Shared by the read buffer and
/// the write buffer's served-prefix read. The holder pins the segment, so a short read is
/// a hard I/O error — throw, never drop a hit.
void preadSegmentNode(
    ChainedBuffers & result,
    FileSegment & segment,
    size_t overlap_start,
    size_t overlap_size,
    size_t object_file_offset,
    const ThrottlerPtr & local_throttler,
    ReaderAnchorCache * anchors)
{
    String path = segment.getPath();
    const size_t offset_in_file = overlap_start - segment.range().left;

    auto buf = std::make_shared<OwnedChainedBuffer>(overlap_size);

    ReadSettings cache_file_read_settings;
    cache_file_read_settings.local_fs_settings.method = LocalFSReadMethod::pread;
    cache_file_read_settings.local_fs_settings.buffer_size = 0;
    cache_file_read_settings.local_throttler = local_throttler;
    const auto open_cache_file = [&](const String & file_path)
    {
        return createReadBufferFromFileBase(
            file_path, cache_file_read_settings,
            /*read_hint=*/std::nullopt,
            /*file_size=*/std::nullopt,
            segment.getFlagsForLocalRead());
    };
    std::shared_ptr<ReadBufferFromFileBase> reader;
    try
    {
        reader = open_cache_file(path);
    }
    catch (const Exception & e)
    {
        /// A fully downloaded segment's file is renamed from `<offset>` to
        /// `<offset>_<size>` on completion, and `getPath` is lock-free, so the name
        /// computed above can go stale between it and the open. The rename surfaces
        /// only as `FILE_DOESNT_EXIST`; recompute the path under the segment lock -
        /// the rename runs under the same lock, so this observes the final name -
        /// and retry once. An unchanged path means the missing file is not explained
        /// by a rename: propagate.
        if (e.code() != ErrorCodes::FILE_DOESNT_EXIST)
            throw;
        String current_path;
        {
            auto segment_lock = segment.lock();
            current_path = segment.getPath();
        }
        if (current_path == path)
            throw;
        path = current_path;
        reader = open_cache_file(path);
    }
    reader->seek(static_cast<off_t>(offset_in_file), SEEK_SET);

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

    result.append(ChainedBufferNode{
        std::move(buf), 0, overlap_size, overlap_start + object_file_offset});

    /// Anchor the reader so its `OpenedFile` stays warm for the next read of this path.
    if (anchors)
        anchors->set(path, reader);
}

/// Object-local end of the bytes safely readable from `segment`: a fully
/// `DOWNLOADED` segment is readable to its inclusive `range().right`, otherwise
/// only up to the live write offset a concurrent downloader has committed.
size_t segmentCommittedEnd(const FileSegment & segment)
{
    return segment.state() == FileSegmentState::DOWNLOADED
        ? segment.range().right + 1
        : segment.getCurrentWriteOffset();
}

/// Append every committed sub-range of `holder` overlapping `sub_in_object`
/// (object-local) to `result`, in segment order, via `preadSegmentNode`. Shared by the read buffer
/// and the write buffer's served-prefix read.
void readOverlappingSegments(
    ChainedBuffers & result,
    FileSegmentsHolder & holder,
    ByteRange sub_in_object,
    size_t object_file_offset,
    const ThrottlerPtr & local_throttler,
    ReaderAnchorCache * anchors)
{
    for (const auto & segment : holder)
    {
        const auto state = segment->state();
        if (state != FileSegmentState::DOWNLOADED
            && state != FileSegmentState::PARTIALLY_DOWNLOADED
            && state != FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION
            && state != FileSegmentState::DOWNLOADING)
            continue;

        const auto & seg_range = segment->range();
        const size_t seg_left = seg_range.left;
        const size_t downloaded_end = segmentCommittedEnd(*segment);

        if (downloaded_end <= sub_in_object.offset || seg_left >= sub_in_object.end())
            continue;

        const size_t overlap_start = std::max<size_t>(seg_left, sub_in_object.offset);
        const size_t overlap_end = std::min(downloaded_end, sub_in_object.end());
        if (overlap_end <= overlap_start)
            continue;

        preadSegmentNode(
            result, *segment, overlap_start, overlap_end - overlap_start,
            object_file_offset, local_throttler, anchors);
    }
}

}

DiskCacheReader::DiskCacheReader(
    std::shared_ptr<FileSegmentsHolder> holder_,
    ByteRange range_in_file,
    size_t object_file_offset_,
    ThrottlerPtr local_throttler_,
    ReaderAnchorCache * anchors_)
    : holder(std::move(holder_))
    , hit_range(range_in_file)
    , object_file_offset(object_file_offset_)
    , local_throttler(std::move(local_throttler_))
    , anchors(anchors_)
{
}

ChainedBuffers DiskCacheReader::read(ByteRange subrange)
{
    ChainedBuffers result;
    if (!holder)
        return result;

    /// Clamp to THIS buffer's hit range: every hit buffer of a view shares one
    /// holder spanning all hit segments, so a `read` for a `subrange` outside `hit_range`
    /// would serve a neighbouring hit's bytes from the shared holder. The contract is
    /// `subrange` within `range()`; clamp defensively to the committed sub-ranges.
    {
        const size_t lo = std::max(subrange.offset, hit_range.offset);
        const size_t hi = std::min(subrange.end(), hit_range.end());
        if (lo >= hi)
            return result;
        subrange = ByteRange{lo, hi - lo};
    }

    /// Record what we serve so the destructor can bump its LRU priority. Record before the pread, so a
    /// throwing read still leaves a coherent record.
    hits_to_touch.push_back(subrange);

    chassert(subrange.offset >= object_file_offset);
    ByteRange sub_in_object{subrange.offset - object_file_offset, subrange.size};

    readOverlappingSegments(result, *holder, sub_in_object, object_file_offset,
        local_throttler, anchors);
    return result;
}

DiskCacheWriter::DiskCacheWriter(
    FileCachePtr cache_,
    size_t object_file_offset_,
    const FilesystemCacheSettings & cache_settings_,
    std::shared_ptr<FileSegmentsHolder> holder_,
    ByteRange aligned_range_in_file)
    : cache(std::move(cache_))
    , object_file_offset(object_file_offset_)
    , cache_settings(cache_settings_)
    , holder(std::move(holder_))
    , aligned_range(aligned_range_in_file)
{
}

size_t DiskCacheWriter::write(ChainedBuffers data)
{
    if (cache_settings.read_if_exists_otherwise_bypass)
        return 0;
    if (!holder)
        return 0;

    /// `FileSegment::range()` is object-local; shift `data` so `ChainedBuffers::copyTo`
    /// sees matching coordinates.
    data.shift(-static_cast<ssize_t>(object_file_offset));

    chassert(aligned_range.offset >= object_file_offset);
    const size_t miss_obj_off = aligned_range.offset - object_file_offset;
    const size_t miss_obj_end = miss_obj_off + aligned_range.size;

    /// Iterate the HELD holder's segments overlapping the still-uncommitted part,
    /// appending append-only at each segment's live `cwo`, but never popping the
    /// segment from the holder (this buffer must keep it appendable across
    /// windows). NEVER throws on the soft skips (detached / unclaimed / no-op).
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

        /// Only segments this thread claimed accept bytes; a role another thread freed is picked up
        /// by the NEXT claim, not here - so open claims fully describe this thread's roles.
        if (!segment.isDownloader())
        {
            LOG_TRACE(log, "DiskCacheWriter::write: segment [{}, {}] not claimed by this thread, skipping",
                seg_range.left, seg_range.right);
            continue;
        }

        /// Append-only: start at the live `cwo`.
        const size_t write_offset = segment.getCurrentWriteOffset();
        const size_t seg_end = seg_range.right + 1;
        const size_t write_end_max = std::min<size_t>(seg_end, miss_obj_end);
        if (write_offset >= write_end_max || write_offset < miss_obj_off)
            continue;

        /// A gap inside `data` caps the write; the segment stays claimed (partial) for
        /// continuation in a later call under the same claim.
        const ByteRange target{write_offset, write_end_max - write_offset};
        size_t contiguous = target.size;
        if (auto data_gaps = data.gaps(target); !data_gaps.empty())
        {
            const size_t first_gap_offset = data_gaps.front().offset;
            contiguous = (first_gap_offset > write_offset) ? (first_gap_offset - write_offset) : 0;
        }
        if (contiguous == 0)
            continue;

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
            continue;
        }

        const bool written_ok = tryWriteToSegment(segment, flat_buf.data(), contiguous, write_offset);
        /// NEVER completeAndPopFront: keep the segment in our holder, appendable next call.
        /// The downloader role stays with the open claim (its destructor finalizes); only
        /// wake the readers waiting on the committed prefix.
        segment.notifyDownloadProgress();

        if (!written_ok)
            continue;

        /// File-level committed interval.
        {
            std::lock_guard lock(committed_mutex);
            committed_ranges.add(ByteRange{write_offset + object_file_offset, contiguous});
        }
        bytes_written += contiguous;

        LOG_TRACE(log, "DiskCacheWriter::write: wrote {} bytes to [{}, {}] at offset {}",
            contiguous, seg_range.left, seg_range.right, write_offset);
    }
    return bytes_written;
}

ChainedBuffers DiskCacheWriter::read(ByteRange subrange)
{
    ChainedBuffers result;
    if (!holder)
        return result;

    chassert(subrange.offset >= object_file_offset);
    ByteRange sub_in_object{subrange.offset - object_file_offset, subrange.size};

    /// Serve an already-committed prefix from this buffer's own held holder,
    /// downloader-independent (a fresh pread reader, unthrottled, unanchored).
    readOverlappingSegments(result, *holder, sub_in_object, object_file_offset,
        /*local_throttler=*/nullptr, /*anchors=*/nullptr);
    return result;
}

CacheWriter::FillClaim DiskCacheWriter::claim(ByteRange window)
{
    /// `window` is FILE-space, clamped per segment. `getCurrentWriteOffset` splits each held segment's
    /// overlap into the committed prefix (`available`) and the uncommitted tail; `getOrSetDownloader`
    /// decides whether that tail is ours (`to_fetch`) or another downloader's (left unlisted). Only
    /// roles NEWLY won here enter the release set - else a leaked DOWNLOADING segment aborts the
    /// foreground holder dtor on `chassert(!is_last_holder)` (it cannot reset a foreign downloader).
    FillClaim c;
    if (!holder)
    {
        c.to_fetch.push_back(window);
        return c;
    }

    auto won = std::make_shared<VectorWithMemoryTracking<FileSegmentPtr>>();
    for (const auto & segment_ptr : *holder)
    {
        FileSegment & segment = *segment_ptr;
        const auto & seg_range = segment.range();
        const size_t seg_file_lo = seg_range.left + object_file_offset;
        const size_t seg_file_hi = seg_range.right + 1 + object_file_offset;

        const size_t lo = std::max(window.offset, seg_file_lo);
        const size_t hi = std::min(window.end(), seg_file_hi);
        if (lo >= hi)
            continue;
        if (segment.isDetached())
            continue;

        if (segment.state() == FileSegmentState::DOWNLOADED)
        {
            c.available.push_back(ByteRange{lo, hi - lo});   // fully cached: whole overlap readable now
            continue;
        }

        const bool already_mine = segment.isDownloader();
        if (!already_mine)
            segment.getOrSetDownloader();

        /// Read `cwo` after the role decision. If we hold the role, only we advance it, so the
        /// committed-prefix / tail split is exact; if another downloader holds it, `cwo` is a lower
        /// bound, so we under-report `available`, never over. `cwo` is object-local; shift to file space.
        const size_t cwo_file = segment.getCurrentWriteOffset() + object_file_offset;
        const size_t avail_hi = std::min(hi, cwo_file);
        if (avail_hi > lo)
            c.available.push_back(ByteRange{lo, avail_hi - lo});

        /// Record a won role for release even with nothing to fetch: holding it would otherwise
        /// self-deadlock a later `waitAndRead` on this thread.
        if (segment.isDownloader())
        {
            const size_t fetch_lo = std::max(lo, cwo_file);
            if (fetch_lo < hi)
                c.to_fetch.push_back(ByteRange{fetch_lo, hi - fetch_lo});
            if (!already_mine)
                won->push_back(segment_ptr);
        }
    }

    if (!won->empty())
    {
        /// Captures the segments (shared refs into the cache), not the writer: the release
        /// stays valid however long the claim is held. Never throws - a failed completion
        /// must not mask the fetch path's own error.
        c.release = [won, logger = log]() noexcept
        {
            for (const auto & segment_ptr : *won)
            {
                try
                {
                    if (!segment_ptr->isDetached() && segment_ptr->isDownloader())
                        segment_ptr->completePartAndResetDownloader();
                }
                catch (...)
                {
                    tryLogCurrentException(logger, "Failed to release a claimed cache segment");
                }
            }
        };
    }
    return c;
}

ChainedBuffers DiskCacheWriter::waitAndRead(ByteRange subrange)
{
    /// `subrange` is FILE-space. Wait until each held segment overlapping it has committed
    /// through the overlap end, then serve the bytes from our own held segments. The caller
    /// orders this AFTER its own led writes, so a cross-thread wait cannot deadlock.
    if (holder)
    {
        for (const auto & segment_ptr : *holder)
        {
            FileSegment & segment = *segment_ptr;
            const auto & seg_range = segment.range();
            const size_t seg_file_lo = seg_range.left + object_file_offset;
            const size_t seg_file_hi = seg_range.right + 1 + object_file_offset;

            const size_t lo = std::max(subrange.offset, seg_file_lo);
            const size_t hi = std::min(subrange.end(), seg_file_hi);
            if (lo >= hi)
                continue;

            const auto st = segment.state();
            if (st != FileSegmentState::DOWNLOADED
                && st != FileSegmentState::PARTIALLY_DOWNLOADED
                && st != FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION
                && st != FileSegmentState::DOWNLOADING)
                continue;

            /// `wait(offset, timeout)` blocks until `offset < getCurrentWriteOffset()`, i.e. the
            /// segment has committed strictly past `offset`, or the timeout expires. We need bytes
            /// through `hi` (object-local `want_obj_end`), so wait on `want_obj_end - 1`. On a
            /// timeout `read` below serves only the committed prefix, so the read can be short.
            chassert(hi >= object_file_offset);
            const size_t want_obj_end = hi - object_file_offset;
            if (want_obj_end > 0)
                segment.wait(want_obj_end - 1, cache_settings.wait_for_concurrent_download_timeout_milliseconds);
        }
    }

    return read(subrange);
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

DiskCacheReader::~DiskCacheReader()
{
    /// Deferred LRU bump of the segments this reader served, so a hit next to fresh inserts isn't
    /// aged below them. Bump on the held `holder` directly - it pins those segments, so no
    /// re-`cache->get`. Sorted records let one linear sweep bump each segment once.
    if (hits_to_touch.empty() || !holder)
        return;

    std::sort(hits_to_touch.begin(), hits_to_touch.end(),
        [](const ByteRange & a, const ByteRange & b) { return a.offset < b.offset; });

    try
    {
        size_t ti = 0;
        for (const auto & segment : *holder)
        {
            const auto state = segment->state();
            if (state != FileSegmentState::DOWNLOADED
                && state != FileSegmentState::PARTIALLY_DOWNLOADED
                && state != FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
                continue;

            /// `segment->range()` is object-local; the recorded ranges are file-space.
            const auto & seg_range = segment->range();
            const size_t seg_start = seg_range.left + object_file_offset;
            const size_t seg_end = seg_range.right + 1 + object_file_offset;

            while (ti < hits_to_touch.size() && hits_to_touch[ti].end() <= seg_start)
                ++ti;
            if (ti < hits_to_touch.size() && hits_to_touch[ti].offset < seg_end)
                segment->increasePriority();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Deferred LRU priority bump failed", LogsLevel::debug);
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
    /// Register a per-query context; null when no download budget is configured
    /// (`filesystem_cache_max_download_size == 0` / no query limit), the unbounded path.
    query_context_holder = cache->getQueryContextHolder(query_id_, cache_settings);
}

/// The disk tier's residency walk. One `resolve` = one cache transaction, no per-call state (shared
/// across `readBigAt` threads). Mirrors the legacy reader's get/getOrSet split. POPULATING: one
/// `getOrSet` - the cache's `splitRange` shapes virgin segments (cells = segments), hits carry
/// readers, misses carry OPEN writers on one shared holder. READ-ONLY / bypass
/// (`read_if_exists_otherwise_bypass`): `cache->get` only - existing segments, gaps and tails as
/// exact writer-less misses, nothing created.
VectorWithMemoryTracking<ICacheProvider::Resolution> DiskCacheProvider::resolve(
    const StoredObject & object, size_t object_file_offset, ByteRange range)
{
    VectorWithMemoryTracking<ICacheProvider::Resolution> out;
    const size_t object_size = object.bytes_size;
    chassert(range.offset >= object_file_offset);
    const size_t ask_lo_obj = range.offset - object_file_offset;
    if (ask_lo_obj >= object_size)
        return out;

    const size_t ask_hi_obj = std::min(range.end() - object_file_offset, object_size);

    auto resolved_key = custom_cache_key.value_or(FileCacheKey::fromPath(object.remote_path));
    auto resolved_origin = custom_origin.value_or(cache->getCommonOriginWithSegmentKeyType(object.local_path));

    /// READ-ONLY / bypass (`cache->get`): existing segments only - hits at their committed extent,
    /// gaps and uncommitted tails as writer-less misses over their EXACT (unrounded) extent within
    /// the ask. The bypass side never fills, so a miss carries no fill-cell geometry - it only tells
    /// the executor which bytes to read from source. Nothing created/reserved/evicted, so a bypass
    /// read (a merge) never perturbs the cache.
    if (!populatesOnMiss())
    {
        auto holder = std::make_shared<FileSegmentsHolder>();
        if (ask_hi_obj > ask_lo_obj)
            if (auto got = cache->get(
                    resolved_key, ask_lo_obj, ask_hi_obj - ask_lo_obj,
                    /*file_segments_limit=*/0, resolved_origin.user_id))
                holder = std::shared_ptr<FileSegmentsHolder>(std::move(got));

        /// The exact uncached extent, clamped to the ask; no rounding since nothing is filled here.
        auto emit_miss = [&](size_t lo_obj, size_t hi_obj)
        {
            const size_t lo = std::max(lo_obj, ask_lo_obj);
            const size_t hi = std::min(hi_obj, ask_hi_obj);
            if (hi <= lo)
                return;
            ICacheProvider::Resolution miss;
            miss.kind = ICacheProvider::Resolution::Kind::Miss;
            miss.range = ByteRange{lo + object_file_offset, hi - lo};
            out.push_back(std::move(miss));
        };

        size_t walk = ask_lo_obj;
        for (const auto & segment_ptr : *holder)
        {
            const auto & seg_range = segment_ptr->range();
            const size_t seg_left = seg_range.left;
            const size_t seg_end = seg_range.right + 1;
            if (seg_left > walk)
                emit_miss(walk, seg_left);
            const size_t committed_end = segmentCommittedEnd(*segment_ptr);
            if (committed_end > seg_left)
            {
                ICacheProvider::Resolution hit;
                hit.kind = ICacheProvider::Resolution::Kind::Hit;
                hit.range = ByteRange{seg_left + object_file_offset, committed_end - seg_left};
                hit.reader = std::make_unique<DiskCacheReader>(
                    holder, hit.range, object_file_offset,
                    local_throttler, &reader_anchors);
                out.push_back(std::move(hit));
            }
            if (committed_end < seg_end)
                emit_miss(committed_end, seg_end);
            walk = std::max(walk, seg_end);
        }
        if (walk < ask_hi_obj)
            emit_miss(walk, ask_hi_obj);
        return out;
    }

    /// One transaction over the (object-clamped) ask: existing segments come
    /// back whole, virgin territory comes back as demand-shaped segments cut by
    /// the cache's `splitRange` on its own grid - the edge overhang is the
    /// grid rounding `getOrSet` itself applies.
    auto shared_holder = std::shared_ptr<FileSegmentsHolder>(cache->getOrSet(
        resolved_key,
        ask_lo_obj,
        ask_hi_obj - ask_lo_obj,
        object_size,
        CreateFileSegmentSettings{},
        /*file_segments_limit=*/0,
        resolved_origin,
        cache_settings.boundary_alignment));

    for (const auto & segment_ptr : *shared_holder)
    {
        FileSegment & segment = *segment_ptr;
        const auto & seg_range = segment.range();
        const size_t seg_left = seg_range.left;
        const size_t seg_end = seg_range.right + 1;
        const size_t committed_end = segmentCommittedEnd(segment);

        if (committed_end > seg_left)
        {
            ICacheProvider::Resolution hit;
            hit.kind = ICacheProvider::Resolution::Kind::Hit;
            hit.range = ByteRange{seg_left + object_file_offset, committed_end - seg_left};
            hit.reader = std::make_unique<DiskCacheReader>(
                shared_holder, hit.range, object_file_offset,
                local_throttler, &reader_anchors);
            out.push_back(std::move(hit));
        }
        if (committed_end < seg_end)
        {
            ICacheProvider::Resolution miss;
            miss.kind = ICacheProvider::Resolution::Kind::Miss;
            /// The cell is the WHOLE segment (the writer appends from the live
            /// committed frontier; the prefix hit above serves the committed part).
            miss.range = ByteRange{seg_left + object_file_offset, seg_end - seg_left};
            miss.writer = std::make_unique<DiskCacheWriter>(
                cache, object_file_offset, cache_settings,
                shared_holder, miss.range);
            out.push_back(std::move(miss));
        }
    }
    return out;
}

}
