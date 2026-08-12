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
#include <Common/CurrentMetrics.h>
#include <algorithm>
#include <cstring>
#include <vector>

namespace CurrentMetrics
{
    /// A segment held (unreleasable) by a DiskCacheReader/DiskCacheWriter after `resolve`
    /// handed it out and `release`d the holder. Kept in step with the buffer's lifetime so
    /// the gauge still reflects the reader-executor's holds.
    extern const Metric FilesystemCacheHoldFileSegments;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CACHE_CANNOT_WRITE_TO_CACHE_DISK;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}

std::shared_ptr<ReadBufferFromFileBase> StreamingReaderSlot::tryCheckout(const String & p, size_t offset)
{
    std::lock_guard lock(mutex);
    if (checked_out || !reader || path != p || next_position != offset)
        return nullptr;
    checked_out = true;
    return reader;
}

void StreamingReaderSlot::checkin(const String & p, std::shared_ptr<ReadBufferFromFileBase> r, size_t next_pos)
{
    std::lock_guard lock(mutex);
    path = p;
    reader = std::move(r);
    next_position = next_pos;
    checked_out = false;
}

void StreamingReaderSlot::abandon()
{
    std::lock_guard lock(mutex);
    reader = nullptr;
    checked_out = false;
}

namespace
{

/// Shared zero-copy pread of `[overlap_start, overlap_start + overlap_size)`
/// (object-local) out of `segment`, appending a single file-level `ChainedBufferNode`
/// (logical offset `overlap_start + object_file_offset`) to `result`. Optionally
/// reuses / refreshes a `StreamingReaderSlot` and anchors the reader. Shared by
/// the read buffer and the write buffer's served-prefix read. The holder pins
/// the segment, so a short read is a hard I/O error — throw, never drop a hit.
void preadSegmentNode(
    ChainedBuffers & result,
    FileSegment & segment,
    size_t overlap_start,
    size_t overlap_size,
    size_t object_file_offset,
    const ThrottlerPtr & local_throttler,
    ReaderAnchorCache * anchors,
    StreamingReaderSlot * stream_slot)
{
    String path = segment.getPath();
    const size_t offset_in_file = overlap_start - segment.range().left;

    auto buf = std::make_shared<OwnedChainedBuffer>(overlap_size);

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
        const auto open_cache_file = [&](const String & file_path)
        {
            return createReadBufferFromFileBase(
                file_path, cache_file_read_settings,
                /*read_hint=*/std::nullopt,
                /*file_size=*/std::nullopt,
                segment.getFlagsForLocalRead());
        };
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

    result.append(ChainedBufferNode{
        std::move(buf), 0, overlap_size, overlap_start + object_file_offset});

    /// A reader reused from the slot is already kept warm by the slot, so
    /// re-anchoring it every window is a redundant locked `CacheBase` insert (plus
    /// the `path` key copy). Anchor only freshly opened readers: the anchor cache
    /// earns its keep across DIFFERENT segment paths / the `readBigAt` fan-out,
    /// where the single slot cannot.
    const bool reused_from_slot = from_slot;
    if (stream_slot)
    {
        stream_slot->checkin(path, reader, offset_in_file + overlap_size);
        from_slot = false;  /// disarm: the reader is handed back, no longer checked out
    }
    if (anchors && !reused_from_slot)
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

/// Read the part of `sub_in_object` (object-local) that `segment` holds committed, into
/// `result`, via `preadSegmentNode`. Shared by the read buffer and the write buffer's
/// served-prefix read; they differ only in whether a streaming-reader slot / anchors /
/// throttler are supplied. The buffer owns exactly one segment - no loop over a holder.
void readSegmentInto(
    ChainedBuffers & result,
    FileSegment & segment,
    ByteRange sub_in_object,
    size_t object_file_offset,
    const ThrottlerPtr & local_throttler,
    ReaderAnchorCache * anchors,
    StreamingReaderSlot * stream_slot)
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
        object_file_offset, local_throttler, anchors, stream_slot);
}

}

DiskCacheReader::DiskCacheReader(
    FileSegmentPtr segment_,
    ByteRange range_in_file,
    size_t object_file_offset_,
    ThrottlerPtr local_throttler_,
    ReaderAnchorCache * anchors_,
    StreamingReaderSlot * stream_slot_)
    : segment(std::move(segment_))
    , hit_range(range_in_file)
    , object_file_offset(object_file_offset_)
    , local_throttler(std::move(local_throttler_))
    , anchors(anchors_)
    , stream_slot(stream_slot_)
{
    if (segment)
        CurrentMetrics::add(CurrentMetrics::FilesystemCacheHoldFileSegments);
}

ChainedBuffers DiskCacheReader::read(ByteRange sub)
{
    ChainedBuffers result;
    if (!segment)
        return result;

    /// Clamp to THIS buffer's hit range - the committed prefix of its one segment.
    /// A `sub` outside `hit_range` is out of contract; clamp defensively.
    {
        const size_t lo = std::max(sub.offset, hit_range.offset);
        const size_t hi = std::min(sub.end(), hit_range.end());
        if (lo >= hi)
            return result;
        sub = ByteRange{lo, hi - lo};
    }

    /// Record before reading: a throwing pread still leaves a coherent entry
    /// that the d-tor bump re-checks and no-ops for a gone segment.
    touched.push_back(sub);

    chassert(sub.offset >= object_file_offset);
    ByteRange sub_in_object{sub.offset - object_file_offset, sub.size};

    readSegmentInto(result, *segment, sub_in_object, object_file_offset,
        local_throttler, anchors, stream_slot);
    return result;
}

DiskCacheWriter::DiskCacheWriter(
    FileCachePtr cache_,
    size_t object_file_offset_,
    const FilesystemCacheSettings & cache_settings_,
    FileSegmentPtr segment_,
    ByteRange aligned_range_in_file)
    : cache(std::move(cache_))
    , object_file_offset(object_file_offset_)
    , cache_settings(cache_settings_)
    , segment(std::move(segment_))
    , aligned_range(aligned_range_in_file)
{
    if (segment)
        CurrentMetrics::add(CurrentMetrics::FilesystemCacheHoldFileSegments);
}

size_t DiskCacheWriter::write(ChainedBuffers data)
{
    if (cache_settings.read_if_exists_otherwise_bypass)
        return 0;
    if (!segment)
        return 0;

    /// `FileSegment::range()` is object-local; shift `data` so `ChainedBuffers::copyTo`
    /// sees matching coordinates.
    data.shift(-static_cast<ssize_t>(object_file_offset));

    chassert(aligned_range.offset >= object_file_offset);
    const size_t miss_obj_off = aligned_range.offset - object_file_offset;
    const size_t miss_obj_end = miss_obj_off + aligned_range.size;

    /// Append-only at our one segment's live `cwo`; never pop it (this buffer keeps it
    /// appendable across windows). NEVER throws on the soft skips (out-of-range /
    /// detached / unclaimed / no-op).
    FileSegment & seg = *segment;
    const auto & seg_range = seg.range();

    if (seg_range.right + 1 <= miss_obj_off || seg_range.left >= miss_obj_end)
        return 0;
    if (seg.isDetached())
        return 0;

    /// `claim` is the sole role-acquisition site: only a segment this thread claimed
    /// accepts bytes. A role a sibling freed mid-window is NOT adopted here - the next
    /// claim takes it - so the roles this thread holds always equal its open claims'
    /// won sets, and the claims' destructors are the complete release story.
    if (!seg.isDownloader())
    {
        LOG_TRACE(log, "DiskCacheWriter::write: segment [{}, {}] not claimed by this thread, skipping",
            seg_range.left, seg_range.right);
        return 0;
    }

    /// Append-only: start at the live `cwo`.
    const size_t write_offset = seg.getCurrentWriteOffset();
    const size_t seg_end = seg_range.right + 1;
    const size_t write_end_max = std::min<size_t>(seg_end, miss_obj_end);
    if (write_offset >= write_end_max || write_offset < miss_obj_off)
        return 0;

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
        return 0;

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
    /// NEVER completeAndPopFront: keep the segment appendable for the next call. The
    /// downloader role stays with the open claim (its release finalizes); only wake the
    /// readers waiting on the committed prefix.
    seg.notifyDownloadProgress();

    if (!written_ok)
        return 0;

    /// File-level committed interval.
    {
        std::lock_guard lock(committed_mutex);
        committed_ranges.add(ByteRange{write_offset + object_file_offset, contiguous});
    }

    LOG_TRACE(log, "DiskCacheWriter::write: wrote {} bytes to [{}, {}] at offset {}",
        contiguous, seg_range.left, seg_range.right, write_offset);
    return contiguous;
}

ChainedBuffers DiskCacheWriter::read(ByteRange sub)
{
    ChainedBuffers result;
    if (!segment)
        return result;

    chassert(sub.offset >= object_file_offset);
    ByteRange sub_in_object{sub.offset - object_file_offset, sub.size};

    /// Serve an already-committed prefix from this buffer's own segment,
    /// downloader-independent (a fresh pread reader, no `StreamingReaderSlot`).
    readSegmentInto(result, *segment, sub_in_object, object_file_offset,
        /*local_throttler=*/nullptr, /*anchors=*/nullptr, /*stream_slot=*/nullptr);
    return result;
}

CacheWriter::FillClaim DiskCacheWriter::claim(ByteRange window)
{
    /// `window` is FILE-space, clamped to this writer's one segment. The already-committed
    /// prefix is `available` (readable from cache now, whoever holds the role; a DOWNLOADED
    /// segment is wholly `available`). For the uncommitted tail `getOrSetDownloader` either makes
    /// us the downloader (`to_fetch`: fetch+write it while the claim is open) or a sibling leads
    /// it - then the tail is CONTENDED and left unlisted (the caller derives it as the window
    /// minus `available` minus `to_fetch`). `claim` is the SOLE role-acquisition site and is
    /// never nested: a caller holds exactly one claim per write (the fetch step's per-tile commit
    /// and the collect put step each hold their own), so this is always a FRESH acquire. Winning
    /// the role arms the release, which completes-and-resets the segment - a claim whose fetch
    /// never reached the segment would otherwise leak it DOWNLOADING, and the teardown cannot
    /// reset a foreign downloader, aborting the holder dtor on `chassert(!is_last_holder)`.
    FillClaim c;
    if (!segment)
    {
        c.to_fetch.push_back(window);
        return c;
    }

    FileSegment & seg = *segment;
    const auto & seg_range = seg.range();
    const size_t seg_file_lo = seg_range.left + object_file_offset;
    const size_t seg_file_hi = seg_range.right + 1 + object_file_offset;

    const size_t lo = std::max(window.offset, seg_file_lo);
    const size_t hi = std::min(window.end(), seg_file_hi);
    if (lo >= hi || seg.isDetached())
        return c;

    if (seg.state() == FileSegmentState::DOWNLOADED)
    {
        /// Fully cached: the whole overlap is readable now.
        c.available.push_back(ByteRange{lo, hi - lo});
        return c;
    }

    /// Never nested (see above): we do not hold this segment's role coming in - assert it.
    /// `getOrSetDownloader` makes us the downloader unless a sibling leads it, and returns the
    /// current downloader's id; we captured the role iff that is us. One call - no `isDownloader`.
    chassert(!seg.isDownloader());
    const bool role_captured = seg.getOrSetDownloader() == FileSegment::getCallerId();

    /// Read `cwo` AFTER the role decision: if we hold the role only WE advance it, so the
    /// committed prefix `[lo, cwo)` / tail `[cwo, hi)` split is exact; if a sibling holds it,
    /// `cwo` is a monotone lower bound - we under-report `available`, never over. `cwo` is
    /// object-local; shift to file space. The committed prefix is readable now regardless of
    /// who holds the role, so it is always `available` (an EMPTY segment has `cwo` at its
    /// start, so `available` is empty - the whole overlap is the tail).
    const size_t cwo_file = seg.getCurrentWriteOffset() + object_file_offset;
    const size_t avail_hi = std::min(hi, cwo_file);
    if (avail_hi > lo)
        c.available.push_back(ByteRange{lo, avail_hi - lo});
    const size_t fetch_lo = std::max(lo, cwo_file);

    /// If we won the role, record `to_fetch` and ARM THE RELEASE - even when the committed prefix
    /// already covers the overlap (`fetch_lo >= hi`, nothing to fetch), winning the role still
    /// leaves us holding it, and a leaked role self-deadlocks a later `waitAndReadSiblingLed` (a
    /// thread cannot wait on a segment it downloads). A tail a sibling leads is CONTENDED - left
    /// unlisted (the caller derives it and neither fetches nor waits on it here).
    if (role_captured)
    {
        if (fetch_lo < hi)
            c.to_fetch.push_back(ByteRange{fetch_lo, hi - fetch_lo});
        /// Captures the segment (a shared ref into the cache), not the writer: the release stays
        /// valid however long the claim is held. Never throws - a failed completion must not mask
        /// the fetch path's own error.
        c.release = [seg_ptr = segment, logger = log]() noexcept
        {
            try
            {
                if (!seg_ptr->isDetached() && seg_ptr->isDownloader())
                    seg_ptr->completePartAndResetDownloader();
            }
            catch (...)
            {
                tryLogCurrentException(logger, "Failed to release a claimed cache segment");
            }
        };
    }
    return c;
}

ChainedBuffers DiskCacheWriter::waitAndReadSiblingLed(ByteRange sub)
{
    /// `sub` is FILE-space. Wait until our one held segment has committed through the overlap
    /// end, then serve the bytes from it. The caller orders this AFTER its own led writes, so a
    /// cross-thread wait cannot deadlock.
    if (segment)
    {
        FileSegment & seg = *segment;
        const auto & seg_range = seg.range();
        const size_t seg_file_lo = seg_range.left + object_file_offset;
        const size_t seg_file_hi = seg_range.right + 1 + object_file_offset;

        const size_t lo = std::max(sub.offset, seg_file_lo);
        const size_t hi = std::min(sub.end(), seg_file_hi);
        const auto st = seg.state();
        const bool servable = st == FileSegmentState::DOWNLOADED
            || st == FileSegmentState::PARTIALLY_DOWNLOADED
            || st == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION
            || st == FileSegmentState::DOWNLOADING;
        if (lo < hi && servable)
        {
            /// `wait(offset, timeout_ms)` blocks until `offset < getCurrentWriteOffset()`
            /// (the segment has committed strictly past `offset`) or the timeout elapses.
            /// We need bytes through `hi` (object-local `want_obj_end`), so wait on
            /// `want_obj_end - 1`, bounded by the same timeout the legacy reader uses.
            chassert(hi >= object_file_offset);
            const size_t want_obj_end = hi - object_file_offset;
            if (want_obj_end > 0)
                seg.wait(want_obj_end - 1, cache_settings.wait_for_concurrent_download_timeout_milliseconds);
        }
    }

    return read(sub);
}

bool DiskCacheWriter::frontierInPartial(size_t frontier) const
{
    /// True if `frontier` lands inside our held segment still being filled - PARTIAL with some
    /// committed bytes. `frontier` is file-level; shift to object-local. Diagnostic only: the
    /// buffer keeps such a segment non-releasable, so it survives eviction / a cache drop; this
    /// just gates the read-ahead pause failpoint.
    if (!segment || frontier < object_file_offset)
        return false;
    const size_t frontier_obj = frontier - object_file_offset;

    const auto & seg_range = segment->range();
    if (!seg_range.contains(frontier_obj))
        return false;
    const auto state = segment->state();
    const bool partial = state == FileSegmentState::DOWNLOADING
                      || state == FileSegmentState::PARTIALLY_DOWNLOADED
                      || state == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION;
    return partial && segment->getCurrentWriteOffset() > seg_range.left && !segment->isDetached();
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

DiskCacheWriter::~DiskCacheWriter()
{
    /// Complete our one segment - replaces what the shared holder's destructor did per segment.
    /// A partial segment shared with the reader is finalized once, by whichever drops last; the
    /// downloader role was already reset per-window by each `claim`'s release.
    if (segment)
    {
        CurrentMetrics::sub(CurrentMetrics::FilesystemCacheHoldFileSegments);
        FileSegment::complete(std::move(segment), /*allow_background_download=*/true, /*force_shrink_to_downloaded_size=*/false);
    }
}

DiskCacheReader::~DiskCacheReader()
{
    if (!segment)
        return;

    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheHoldFileSegments);

    /// LRU bump: if we actually read any bytes from our segment, raise its priority so a hit
    /// next to fresh inserts isn't aged below them. Bump directly on the pinned segment (no
    /// re-`cache->get`, which would re-hash the key + re-take the per-key metadata lock). Every
    /// `touched` range came from `read`, clamped to `hit_range` inside this segment, so a
    /// non-empty `touched` means the segment was read. A segment still `DOWNLOADING` (a sibling
    /// writer fills its tail) is skipped; the fill itself gives it insert priority.
    try
    {
        if (!touched.empty())
        {
            const auto state = segment->state();
            if (state == FileSegmentState::DOWNLOADED
                || state == FileSegmentState::PARTIALLY_DOWNLOADED
                || state == FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION)
                segment->increasePriority();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Deferred LRU priority bump failed", LogsLevel::debug);
    }

    /// Complete our segment - replaces the holder's per-segment completion. A read-only
    /// DOWNLOADED hit is a no-op (`isCompleted` early-return); a partial segment shared with the
    /// writer is finalized once, by whichever of us drops last (`is_last_holder`).
    FileSegment::complete(std::move(segment), /*allow_background_download=*/true, /*force_shrink_to_downloaded_size=*/false);
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

size_t DiskCacheProvider::resolvedBoundaryAlignment() const
{
    return std::max<size_t>(1, cache_settings.boundary_alignment.value_or(cache->getBoundaryAlignment()));
}

size_t DiskCacheProvider::maxFillCell() const
{
    const size_t boundary = resolvedBoundaryAlignment();
    const size_t max_segment = cache->getMaxFileSegmentSize();
    if (max_segment <= boundary)
        return boundary;
    return max_segment / boundary * boundary;
}

/// The disk tier's residency walk. One `resolve` call is one cache transaction
/// and the method holds no per-call state, so a shared provider is safe to
/// resolve from many threads (the `readBigAt` fan-out). Mirrors the legacy
/// reader's get/getOrSet split
/// (`CachedOnDiskReadBufferFromFile::nextFileSegmentsBatch`). POPULATING
/// (write-through) cache: one `getOrSet` transaction - the cache's own
/// `splitRange` shapes the virgin segments (cells = segments), hits carry
/// readers, misses carry OPEN writers sharing the one holder. READ-ONLY /
/// bypass cache (`read_if_exists_otherwise_bypass`): `cache->get` only -
/// existing segments come back, hits carry readers, gaps and uncommitted tails
/// are boundary-aligned writer-less misses, nothing is created or reserved.
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

    /// READ-ONLY / bypass cache (mirrors the legacy reader's `cache->get` branch
    /// in `CachedOnDiskReadBufferFromFile`): return existing segments only -
    /// hits at their committed extent, gaps and uncommitted tails as
    /// boundary-aligned writer-less misses. Nothing is created, reserved, or
    /// evicted, so a bypass read (a merge) never perturbs the cache.
    /// `observeSpan` drops a bypass tier's misses; only `probeView` reads them.
    if (!populatesOnMiss())
    {
        const size_t boundary = cache_settings.boundary_alignment.value_or(cache->getBoundaryAlignment());
        const size_t ask_start = FileCacheUtils::roundDownToMultiple(ask_lo_obj, boundary);
        const size_t ask_end = std::min(FileCacheUtils::roundUpToMultiple(ask_hi_obj, boundary), object_size);

        auto holder = std::make_shared<FileSegmentsHolder>();
        if (ask_end > ask_start)
            if (auto got = cache->get(
                    resolved_key, ask_start, ask_end - ask_start,
                    /*file_segments_limit=*/0, resolved_origin.user_id))
                holder = std::shared_ptr<FileSegmentsHolder>(std::move(got));

        /// Boundary-align each writer-less miss (clamped to the object end); the
        /// bypass side never fills, so misses carry no fill-cell geometry.
        auto emit_miss = [&](size_t lo_obj, size_t hi_obj)
        {
            const size_t a_off = FileCacheUtils::roundDownToMultiple(lo_obj, boundary);
            const size_t a_end = std::min(FileCacheUtils::roundUpToMultiple(hi_obj, boundary), object_size);
            if (a_end <= a_off)
                return;
            ICacheProvider::Resolution miss;
            miss.kind = ICacheProvider::Resolution::Kind::Miss;
            miss.range = ByteRange{a_off + object_file_offset, a_end - a_off};
            out.push_back(std::move(miss));
        };

        size_t walk = ask_start;
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
                    segment_ptr, hit.range, object_file_offset,
                    local_throttler, &reader_anchors, &streaming_slot);
                out.push_back(std::move(hit));
            }
            if (committed_end < seg_end)
                emit_miss(committed_end, seg_end);
            walk = std::max(walk, seg_end);
        }
        if (walk < ask_end)
            emit_miss(walk, ask_end);
        /// The hit readers copied out their FileSegmentPtrs; drop the holder without completing
        /// (each reader completes its own segment on destruction).
        holder->release();
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
                segment_ptr, hit.range, object_file_offset,
                local_throttler, &reader_anchors, &streaming_slot);
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
                segment_ptr, miss.range);
            out.push_back(std::move(miss));
        }
    }
    /// The hit readers / miss writers copied out their FileSegmentPtrs (a partial segment's
    /// reader and writer share the same one); drop the holder without completing - each buffer
    /// completes its own segment on destruction.
    shared_holder->release();
    return out;
}

}
