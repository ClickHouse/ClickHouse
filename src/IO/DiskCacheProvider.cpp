#include <IO/DiskCacheProvider.h>

#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/CurrentMetrics.h>
#include <base/scope_guard.h>
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

/// Read the part of `sub_in_object` (object-local) that `segment` holds committed into `result`,
/// via `preadSegmentNode`. Shared by the read buffer and the write buffer's served-prefix read; the
/// buffer owns exactly one segment - no loop over a holder.
void readSegmentInto(
    ChainedBuffers & result,
    FileSegment & segment,
    ByteRange sub_in_object,
    size_t object_file_offset,
    const ThrottlerPtr & local_throttler,
    ReaderAnchorCache * anchors)
{
    const auto state = segment.state();
    if (state != FileSegmentState::DOWNLOADED
        && state != FileSegmentState::PARTIALLY_DOWNLOADED
        && state != FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION
        && state != FileSegmentState::DOWNLOADING)
        return;

    const auto & seg_range = segment.range();
    const size_t seg_left = seg_range.left;
    const size_t downloaded_end = segmentCommittedEnd(segment);

    if (downloaded_end <= sub_in_object.offset || seg_left >= sub_in_object.end())
        return;

    const size_t overlap_start = std::max<size_t>(seg_left, sub_in_object.offset);
    const size_t overlap_end = std::min(downloaded_end, sub_in_object.end());
    if (overlap_end <= overlap_start)
        return;

    preadSegmentNode(
        result, segment, overlap_start, overlap_end - overlap_start,
        object_file_offset, local_throttler, anchors);
}

}

DiskCacheReader::DiskCacheReader(
    FileSegmentsHolderSharedPtr segment_holder_,
    ByteRange range_in_file,
    size_t object_file_offset_,
    ThrottlerPtr local_throttler_,
    ReaderAnchorCache * anchors_)
    : segment_holder(std::move(segment_holder_))
    , hit_range(range_in_file)
    , object_file_offset(object_file_offset_)
    , local_throttler(std::move(local_throttler_))
    , anchors(anchors_)
{
    chassert(segment_holder && segment_holder->size() == 1);
    /// The hit range (file-space) must fall inside this segment's file-space span.
    const auto & seg_range = segment().range();
    chassert(hit_range.offset >= seg_range.left + object_file_offset
        && hit_range.end() <= seg_range.right + 1 + object_file_offset);
}

ChainedBuffers DiskCacheReader::read(ByteRange subrange)
{
    ChainedBuffers result;
    /// Clamp to THIS buffer's hit range - the committed prefix of its one segment. A `subrange`
    /// outside `hit_range` is out of contract; clamp defensively.
    {
        const size_t lo = std::max(subrange.offset, hit_range.offset);
        const size_t hi = std::min(subrange.end(), hit_range.end());
        if (lo >= hi)
            return result;
        subrange = ByteRange{lo, hi - lo};
    }

    /// Mark that we served bytes so the destructor bumps this segment's cache priority.
    served = true;

    chassert(subrange.offset >= object_file_offset);
    ByteRange sub_in_object{subrange.offset - object_file_offset, subrange.size};

    readSegmentInto(result, segment(), sub_in_object, object_file_offset,
        local_throttler, anchors);
    return result;
}

DiskCacheWriter::DiskCacheWriter(
    FileCachePtr cache_,
    size_t object_file_offset_,
    const FilesystemCacheSettings & cache_settings_,
    FileSegmentsHolderSharedPtr segment_holder_,
    ByteRange aligned_range_in_file)
    : cache(std::move(cache_))
    , object_file_offset(object_file_offset_)
    , cache_settings(cache_settings_)
    , segment_holder(std::move(segment_holder_))
    , aligned_range(aligned_range_in_file)
{
    chassert(segment_holder && segment_holder->size() == 1);
    /// The aligned miss range (file-space) must fall inside this segment's file-space span.
    const auto & seg_range = segment().range();
    chassert(aligned_range.offset >= seg_range.left + object_file_offset
        && aligned_range.end() <= seg_range.right + 1 + object_file_offset);
}

size_t DiskCacheWriter::write(ChainedBuffers data, const FillRole & role)
{
    if (cache_settings.read_if_exists_otherwise_bypass)
        return 0;

    /// `FileSegment::range()` is object-local; shift `data` so `ChainedBuffers::copyTo`
    /// sees matching coordinates.
    data.shift(-static_cast<ssize_t>(object_file_offset));

    chassert(aligned_range.offset >= object_file_offset);
    const size_t miss_obj_off = aligned_range.offset - object_file_offset;
    const size_t miss_obj_end = miss_obj_off + aligned_range.size;

    /// Append append-only at our one segment's live current write offset, never completing it
    /// here (kept appendable across windows; the role's release / the destructor finalize it). NEVER
    /// throws on the soft skips (unclaimed / no-op).
    FileSegment & seg = segment();
    const auto & seg_range = seg.range();

    /// `takeFillRole` is the sole role-acquisition site; `write` never takes one. So a caller reaches
    /// here only under a held `role` that won this segment's role - assert the caller's proof and the
    /// live state.
    chassert(role);
    chassert(seg.isDownloader());

    /// Append-only: start at the live current write offset.
    const size_t write_offset = seg.getCurrentWriteOffset();
    const size_t seg_end = seg_range.right + 1;
    const size_t write_end_max = std::min<size_t>(seg_end, miss_obj_end);
    if (write_offset >= write_end_max || write_offset < miss_obj_off)
        return 0;

    /// A gap inside `data` caps the write; the segment stays claimed (partial) for continuation in a
    /// later call under the same role.
    const ByteRange target{write_offset, write_end_max - write_offset};
    size_t contiguous = target.size;
    if (auto data_gaps = data.gaps(target); !data_gaps.empty())
    {
        const size_t first_gap_offset = data_gaps.front().offset;
        contiguous = (first_gap_offset > write_offset) ? (first_gap_offset - write_offset) : 0;
    }
    if (contiguous == 0)
        return 0;

    /// Validate + flatten before `reserve`: an exception after `reserve` (which sets
    /// `queue_iterator`) trips the framework's `EMPTY ⇒ !queue_iterator` invariant during cleanup.
    const ByteRange write_range{write_offset, contiguous};
    if (!data.covers(write_range))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskCacheWriter::write: data does not contiguously cover the range being written: "
            "write_range=[{}, {}), data intervals={}",
            write_range.offset, write_range.end(), data.getIntervals().size());

    VectorWithMemoryTracking<char> flat_buf(contiguous);
    data.copyTo(flat_buf.data(), write_range);

    std::string failure_reason;
    const bool reserved = seg.reserve(
        contiguous,
        cache_settings.reserve_space_wait_lock_timeout_milliseconds,
        failure_reason);
    if (!reserved)
    {
        LOG_TRACE(log, "DiskCacheWriter::write: reserve failed for [{}, {}]: {}",
            seg_range.left, seg_range.right, failure_reason);
        return 0;
    }

    const bool written_ok = tryWriteToSegment(seg, flat_buf.data(), contiguous, write_offset);
    /// Keep the segment appendable; the downloader role stays with the open role (its release
    /// finalizes it). Only wake the readers waiting on the committed prefix.
    seg.notifyDownloadProgress();

    if (!written_ok)
        return 0;

    LOG_TRACE(log, "DiskCacheWriter::write: wrote {} bytes to [{}, {}] at offset {}",
        contiguous, seg_range.left, seg_range.right, write_offset);
    return contiguous;
}

ChainedBuffers DiskCacheWriter::read(ByteRange subrange)
{
    ChainedBuffers result;
    chassert(subrange.offset >= object_file_offset);
    ByteRange sub_in_object{subrange.offset - object_file_offset, subrange.size};

    /// Serve an already-committed prefix from this buffer's own segment (a fresh pread reader,
    /// unthrottled, unanchored).
    readSegmentInto(result, segment(), sub_in_object, object_file_offset,
        /*local_throttler=*/nullptr, /*anchors=*/nullptr);
    return result;
}

size_t DiskCacheWriter::committed() const
{
    /// The segment's committed prefix in file space (append-only from its start), clamped to our range.
    /// Reads the segment's live frontier directly - no separate bookkeeping - so it also reflects a
    /// prefix another downloader committed on the same segment.
    const size_t seg_committed_file_end = segmentCommittedEnd(segment()) + object_file_offset;
    return std::clamp(seg_committed_file_end, aligned_range.offset, aligned_range.end());
}

CacheWriter::FillRole DiskCacheWriter::takeFillRole()
{
    /// Our one segment. If it has an uncommitted tail we either win the role (hold the role, fetch+write
    /// it) or a concurrent downloader leads it (hold nothing; the caller reads `committed()` and waits).
    FileSegment & seg = segment();
    const size_t seg_file_end = seg.range().right + 1 + object_file_offset;

    if (seg.state() == FileSegmentState::DOWNLOADED)
        return {};   /// fully cached: readable via committed(), no role

    /// The write offset is readable without the role; if it already covers the segment, nothing to fill.
    if (seg.getCurrentWriteOffset() + object_file_offset >= seg_file_end)
        return {};

    /// Acquire the role for the tail. Never nested (one role per write), so we do not already hold it.
    chassert(!seg.isDownloader());
    const bool won = seg.getOrSetDownloader() == FileSegment::getCallerId();

    /// If we won, release the role on any exit below unless we hand it to the FillRole: a leaked
    /// DOWNLOADING segment aborts the writer's holder dtor and deadlocks a later waitAndRead.
    bool safe_guard_armed = true;
    SCOPE_EXIT({
        if (won && safe_guard_armed)
        {
            try
            {
                seg.completePartAndResetDownloader();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to release a claimed cache segment");
                chassert(false);
            }
        }
    });

    if (!won)
        return {};   /// a concurrent downloader leads the tail: hold nothing, the caller waits on it

    if (seg.getCurrentWriteOffset() + object_file_offset >= seg_file_end)
        return {};   /// filled since the pre-check: nothing left (the guard releases the role)

    /// Hand the role to the FillRole (disarming the guard). The release captures the segment (a shared
    /// ref), not the writer.
    FillRole role = makeFillRole(/*held=*/true, [seg_ptr = segment_holder->getSingleFileSegment(), logger = log]() noexcept
    {
        chassert(seg_ptr->isDownloader());
        try
        {
            seg_ptr->completePartAndResetDownloader();
        }
        catch (...)
        {
            tryLogCurrentException(logger, "Failed to release a claimed cache segment");
            chassert(false);
        }
    });
    safe_guard_armed = false;
    return role;
}

ChainedBuffers DiskCacheWriter::waitAndRead(ByteRange subrange)
{
    /// `subrange` is FILE-space. Wait until our one segment has committed through the overlap end,
    /// then serve the bytes. The caller orders this AFTER its own led writes, so a cross-thread wait
    /// cannot deadlock.
    FileSegment & seg = segment();
    const auto & seg_range = seg.range();
    const size_t seg_file_begin = seg_range.left + object_file_offset;
    const size_t seg_file_end = seg_range.right + 1 + object_file_offset;

    const size_t lo = std::max(subrange.offset, seg_file_begin);
    const size_t hi = std::min(subrange.end(), seg_file_end);
    const auto st = seg.state();
    const bool readable = st == FileSegmentState::DOWNLOADED
        || st == FileSegmentState::PARTIALLY_DOWNLOADED
        || st == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION
        || st == FileSegmentState::DOWNLOADING;
    if (lo < hi && readable)
    {
        /// `wait(offset, timeout)` blocks until `offset < getCurrentWriteOffset()`, i.e. the
        /// segment has committed strictly past `offset`, or the timeout expires. We need bytes
        /// through `hi` (object-local `want_obj_end`), so wait on `want_obj_end - 1`. On a
        /// timeout `read` below serves only the committed prefix, so the read can be short.
        chassert(hi >= object_file_offset);
        const size_t want_obj_end = hi - object_file_offset;
        if (want_obj_end > 0)
            seg.wait(want_obj_end - 1, cache_settings.wait_for_concurrent_download_timeout_milliseconds);
    }

    return read(subrange);
}

bool DiskCacheWriter::tryWriteToSegment(FileSegment & file_segment, char * data, size_t size, size_t offset)
{
    /// `FileSegment::write` leaves the segment in
    /// `PARTIALLY_DOWNLOADED_NO_CONTINUATION` on `ErrnoException`. Disk-full /
    /// quota are fail-open; other errors honour `skipCacheOnDiskFailure`.
    try
    {
        file_segment.write(data, size, offset);
        return true;
    }
    catch (ErrnoException & e)
    {
        const int code = e.getErrno();
        const bool is_no_space_left = code == 28 || code == 122;
        chassert(file_segment.state() == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
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
    /// Deferred priority bump: if we served any bytes, raise the segment's priority so a hit next to
    /// fresh inserts isn't aged below them. A segment still DOWNLOADING (another thread fills its
    /// tail) is skipped; the fill itself gives it insert priority. The `segment_holder` member then
    /// completes the segment as it is destroyed.
    if (!served || !segment_holder)
        return;
    try
    {
        FileSegment & seg = segment();
        const auto state = seg.state();
        if (state == FileSegmentState::DOWNLOADED
            || state == FileSegmentState::PARTIALLY_DOWNLOADED
            || state == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
            seg.increasePriority();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Deferred priority bump failed", LogsLevel::debug);
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
/// `getOrSet` - the cache's `splitRange` shapes virgin segments, hits carry readers, misses carry
/// OPEN writers; each reader/writer gets its own single-segment holder via `popHolder`. READ-ONLY /
/// bypass (`read_if_exists_otherwise_bypass`): `cache->get` only - existing segments, gaps and tails
/// as exact writer-less misses, nothing created.
VectorWithMemoryTracking<ICacheProvider::CacheResolution> DiskCacheProvider::resolve(
    const StoredObject & object, size_t object_offset, ByteRange range)
{
    VectorWithMemoryTracking<ICacheProvider::CacheResolution> out;
    const size_t object_size = object.bytes_size;
    /// Object's file base (see `ICacheProvider::resolve`): lifts object-local extents to file space.
    chassert(range.offset >= object_offset);
    const size_t object_file_offset = range.offset - object_offset;
    const size_t ask_lo_obj = object_offset;
    if (ask_lo_obj >= object_size)
        return out;

    const size_t ask_hi_obj = std::min(range.end() - object_file_offset, object_size);
    if (ask_hi_obj <= ask_lo_obj)
        return out;

    auto resolved_key = custom_cache_key.value_or(FileCacheKey::fromPath(object.remote_path));
    auto resolved_origin = custom_origin.value_or(cache->getCommonOriginWithSegmentKeyType(object.local_path));

    /// A hit over a segment's committed prefix `[seg_left, committed_end)`: a reader on that segment's
    /// own single-segment holder. Shared by both paths - the populate path also hands the same holder
    /// to the miss writer for a partial segment.
    auto emit_hit = [&](const FileSegmentsHolderSharedPtr & seg_holder, size_t seg_left, size_t committed_end)
    {
        ICacheProvider::CacheResolution hit;
        hit.kind = ICacheProvider::CacheResolution::Kind::Hit;
        hit.range = ByteRange{seg_left + object_file_offset, committed_end - seg_left};
        hit.reader = std::make_unique<DiskCacheReader>(
            seg_holder, hit.range, object_file_offset,
            local_throttler, &reader_anchors);
        out.push_back(std::move(hit));
    };

    /// READ-ONLY / bypass (`cache->get`): existing segments only - hits at their committed extent,
    /// gaps and uncommitted tails as writer-less misses over their EXACT (unrounded) extent within
    /// the ask. The bypass side never fills, so a miss carries no fill geometry - it only tells
    /// the executor which bytes to read from source. Nothing created/reserved/evicted, so a bypass
    /// read (a merge) never perturbs the cache.
    if (cache_settings.read_if_exists_otherwise_bypass)
    {
        auto got_holder = cache->get(
            resolved_key, ask_lo_obj, ask_hi_obj - ask_lo_obj,
            /*file_segments_limit=*/0, resolved_origin.user_id);

        /// The exact uncached extent, clamped to the ask; no rounding since nothing is filled here.
        auto emit_miss = [&](size_t lo_obj, size_t hi_obj)
        {
            const size_t lo = std::max(lo_obj, ask_lo_obj);
            const size_t hi = std::min(hi_obj, ask_hi_obj);
            if (hi <= lo)
                return;
            ICacheProvider::CacheResolution miss;
            miss.kind = ICacheProvider::CacheResolution::Kind::Miss;
            miss.range = ByteRange{lo + object_file_offset, hi - lo};
            out.push_back(std::move(miss));
        };

        size_t walk = ask_lo_obj;
        while (got_holder && !got_holder->empty())
        {
            /// Hand each segment its own single-segment holder (which completes it on destruction).
            auto seg_holder = got_holder->popHolder();
            const FileSegment & segment = seg_holder->front();
            const auto & seg_range = segment.range();
            const size_t seg_left = seg_range.left;
            const size_t seg_end = seg_range.right + 1;
            if (seg_left > walk)
                emit_miss(walk, seg_left);
            const size_t committed_end = segmentCommittedEnd(segment);
            if (committed_end > seg_left)
                emit_hit(seg_holder, seg_left, committed_end);
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
    auto got_holder = cache->getOrSet(
        resolved_key,
        ask_lo_obj,
        ask_hi_obj - ask_lo_obj,
        object_size,
        CreateFileSegmentSettings{},
        /*file_segments_limit=*/0,
        resolved_origin,
        cache_settings.boundary_alignment);

    /// A miss over the segment's WHOLE extent, carrying an OPEN writer on the same single-segment
    /// holder as the prefix hit (a partial segment shares the holder). The writer appends from the
    /// live committed frontier; the prefix hit above serves the committed part.
    auto emit_miss = [&](const FileSegmentsHolderSharedPtr & seg_holder, size_t seg_left, size_t seg_end)
    {
        ICacheProvider::CacheResolution miss;
        miss.kind = ICacheProvider::CacheResolution::Kind::Miss;
        miss.range = ByteRange{seg_left + object_file_offset, seg_end - seg_left};
        miss.writer = std::make_unique<DiskCacheWriter>(
            cache, object_file_offset, cache_settings,
            seg_holder, miss.range);
        out.push_back(std::move(miss));
    };

    /// A writer-less miss clamped to the ask: read from source, cache nothing. The clamp keeps a
    /// detached edge cell's grid overhang from being fetched uncached, as the bypass path above does.
    auto emit_uncacheable_miss = [&](size_t seg_left, size_t seg_end)
    {
        const size_t lo = std::max(seg_left, ask_lo_obj);
        const size_t hi = std::min(seg_end, ask_hi_obj);
        if (hi <= lo)
            return;
        ICacheProvider::CacheResolution miss;
        miss.kind = ICacheProvider::CacheResolution::Kind::Miss;
        miss.range = ByteRange{lo + object_file_offset, hi - lo};
        out.push_back(std::move(miss));
    };

    while (!got_holder->empty())
    {
        /// One holder per segment; a partial segment's hit reader and miss writer SHARE it, so the
        /// segment completes once when the last of them drops.
        auto seg_holder = got_holder->popHolder();
        const FileSegment & segment = seg_holder->front();
        const auto & seg_range = segment.range();
        const size_t seg_left = seg_range.left;
        const size_t seg_end = seg_range.right + 1;

        /// A DETACHED placeholder holds no bytes and cannot take a downloader; serve it from source.
        if (segment.isDetached())
        {
            emit_uncacheable_miss(seg_left, seg_end);
            continue;
        }

        const size_t committed_end = segmentCommittedEnd(segment);

        if (committed_end > seg_left)
            emit_hit(seg_holder, seg_left, committed_end);
        if (committed_end < seg_end)
            emit_miss(seg_holder, seg_left, seg_end);
    }
    return out;
}

}
