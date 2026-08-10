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

/// Append every committed sub-range of `holder` overlapping `sub_in_object`
/// (object-local) to `result`, in segment order, via `preadSegmentNode`. Shared by
/// the read buffer and the write buffer's served-prefix read; they differ only in
/// whether a streaming-reader slot / anchors / throttler are supplied.
void readOverlappingSegments(
    ChainedBuffers & result,
    FileSegmentsHolder & holder,
    ByteRange sub_in_object,
    size_t object_file_offset,
    const ThrottlerPtr & local_throttler,
    ReaderAnchorCache * anchors,
    StreamingReaderSlot * stream_slot)
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
            object_file_offset, local_throttler, anchors, stream_slot);
    }
}

}

DiskCacheReader::DiskCacheReader(
    std::shared_ptr<FileSegmentsHolder> holder_,
    ByteRange range_in_file,
    size_t object_file_offset_,
    ThrottlerPtr local_throttler_,
    ReaderAnchorCache * anchors_,
    StreamingReaderSlot * stream_slot_,
    std::shared_ptr<DiskCacheTouchBook> touch_book_)
    : holder(std::move(holder_))
    , hit_range(range_in_file)
    , object_file_offset(object_file_offset_)
    , local_throttler(std::move(local_throttler_))
    , anchors(anchors_)
    , stream_slot(stream_slot_)
    , touch_book(std::move(touch_book_))
{
}

ChainedBuffers DiskCacheReader::read(ByteRange sub)
{
    ChainedBuffers result;
    if (!holder)
        return result;

    /// Clamp to THIS buffer's hit range: every hit buffer of a view shares one
    /// holder spanning all hit segments, so a `read` for a `sub` outside `hit_range`
    /// would serve a neighbouring hit's bytes from the shared holder. The contract is
    /// `sub` within `range()`; clamp defensively to the committed sub-ranges.
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
    if (touch_book)
        touch_book->touched.push_back(sub);

    chassert(sub.offset >= object_file_offset);
    ByteRange sub_in_object{sub.offset - object_file_offset, sub.size};

    readOverlappingSegments(result, *holder, sub_in_object, object_file_offset,
        local_throttler, anchors, stream_slot);
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

        /// `claim` is the sole role-acquisition site: only segments this thread claimed
        /// accept bytes. A role a sibling freed mid-window is NOT adopted here - the next
        /// claim takes it - so the roles this thread holds always equal its open claims'
        /// won sets, and the claims' destructors are the complete release story.
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

ChainedBuffers DiskCacheWriter::read(ByteRange sub)
{
    ChainedBuffers result;
    if (!holder)
        return result;

    chassert(sub.offset >= object_file_offset);
    ByteRange sub_in_object{sub.offset - object_file_offset, sub.size};

    /// Serve an already-committed prefix from this buffer's own held holder,
    /// downloader-independent (a fresh pread reader, no `StreamingReaderSlot`).
    readOverlappingSegments(result, *holder, sub_in_object, object_file_offset,
        /*local_throttler=*/nullptr, /*anchors=*/nullptr, /*stream_slot=*/nullptr);
    return result;
}

CacheWriter::FillClaim DiskCacheWriter::claim(ByteRange window)
{
    /// `window` is FILE-space, clamped per segment. For each held segment overlapping it:
    /// a DOWNLOADED segment is already cached (`sibling_led`: serve from cache, a wait on
    /// it returns immediately); for an undownloaded one, `getOrSetDownloader` either makes
    /// us the downloader or a sibling leads it (`sibling_led`). When we hold the role, the
    /// segment's already-committed prefix is `available` (read from cache, never refetched)
    /// and only the missing tail is `to_fetch`. Only roles NEWLY won here enter the claim's
    /// release set: a nested claim over segments this thread already leads (a tile write
    /// inside a window-long claim) must not release the outer claim's roles. The release
    /// completes-and-resets exactly those segments - a claim whose fetch never reached a
    /// segment would otherwise leak it DOWNLOADING, and the foreground teardown cannot reset
    /// a foreign (this worker's) downloader, aborting the holder dtor on
    /// `chassert(!is_last_holder)`.
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
            c.sibling_led.push_back(ByteRange{lo, hi - lo});
            continue;
        }

        const bool already_mine = segment.isDownloader();
        if (!already_mine)
            segment.getOrSetDownloader();
        if (segment.isDownloader())
        {
            /// We hold the role (won it now, or already had it). Only WE advance `cwo`, so it is
            /// stable here: the committed prefix `[lo, cwo)` is `available` (read from cache, never
            /// refetched from the source - a prior downloader's partial we resumed), the tail
            /// `[cwo, hi)` is `to_fetch`. `cwo` is object-local; shift to file space. An EMPTY
            /// segment has `cwo` at its start, so `available` is empty and the whole overlap is
            /// `to_fetch` - identical to before.
            const size_t cwo_file = segment.getCurrentWriteOffset() + object_file_offset;
            const size_t avail_hi = std::min(hi, cwo_file);
            if (avail_hi > lo)
                c.available.push_back(ByteRange{lo, avail_hi - lo});
            const size_t fetch_lo = std::max(lo, cwo_file);
            if (hi > fetch_lo)
                c.to_fetch.push_back(ByteRange{fetch_lo, hi - fetch_lo});
            if (!already_mine)
                won->push_back(segment_ptr);
        }
        else
        {
            c.sibling_led.push_back(ByteRange{lo, hi - lo});
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

ChainedBuffers DiskCacheWriter::waitAndReadSiblingLed(ByteRange sub)
{
    /// `sub` is FILE-space. Wait until each held segment overlapping it has committed
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

            const size_t lo = std::max(sub.offset, seg_file_lo);
            const size_t hi = std::min(sub.end(), seg_file_hi);
            if (lo >= hi)
                continue;

            const auto st = segment.state();
            if (st != FileSegmentState::DOWNLOADED
                && st != FileSegmentState::PARTIALLY_DOWNLOADED
                && st != FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION
                && st != FileSegmentState::DOWNLOADING)
                continue;

            /// `wait(offset)` blocks until `offset < getCurrentWriteOffset()`, i.e. the
            /// segment has committed strictly past `offset`. We need bytes through `hi`
            /// (object-local `want_obj_end`), so wait on `want_obj_end - 1`.
            chassert(hi >= object_file_offset);
            const size_t want_obj_end = hi - object_file_offset;
            if (want_obj_end > 0)
                segment.wait(want_obj_end - 1);
        }
    }

    return read(sub);
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

        /// A DOWNLOADING segment is pinnable too: a claim holds the downloader role for
        /// its whole lifetime and releases it only after the collect, so the pin decision
        /// can race the release - the same partial segment shows as DOWNLOADING or
        /// PARTIALLY_DOWNLOADED depending on thread timing. The pin is a plain holder
        /// reference; taken while still DOWNLOADING it protects the partial across the
        /// following plan rebuild with no unprotected gap.
        const auto state = segment->state();
        const bool partial = state == FileSegmentState::DOWNLOADING
                          || state == FileSegmentState::PARTIALLY_DOWNLOADED
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

DiskCacheTouchBook::~DiskCacheTouchBook()
{
    /// Deferred LRU bump: raise the priority of each segment the probe's readers
    /// actually read, so a hit next to fresh inserts isn't aged below them. Bump
    /// directly on the held `holder` - it pins exactly the segments the readers
    /// read from, so there is no need to re-`cache->get` them (which would
    /// re-take the per-key metadata lock and re-hash the key). A sequential run
    /// records many contiguous sub-ranges over the same few segments; sorting the
    /// records lets the linear sweep below bump each touched segment exactly
    /// once. Runs at the LAST owner's death - after every reader of the probe is
    /// released (the executor orders write-buffer destruction before that).
    if (touched.empty() || !holder)
        return;

    std::sort(touched.begin(), touched.end(),
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

            /// Drop records lying entirely before this segment.
            while (ti < touched.size() && touched[ti].end() <= seg_start)
                ++ti;
            /// Bump once if the next record overlaps the segment.
            if (ti < touched.size() && touched[ti].offset < seg_end)
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
        auto book = std::make_shared<DiskCacheTouchBook>(holder, object_file_offset);

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
                    holder, hit.range, object_file_offset,
                    local_throttler, &reader_anchors, &streaming_slot, book);
                out.push_back(std::move(hit));
            }
            if (committed_end < seg_end)
                emit_miss(committed_end, seg_end);
            walk = std::max(walk, seg_end);
        }
        if (walk < ask_end)
            emit_miss(walk, ask_end);
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
    auto book = std::make_shared<DiskCacheTouchBook>(shared_holder, object_file_offset);

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
                local_throttler, &reader_anchors, &streaming_slot, book);
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
