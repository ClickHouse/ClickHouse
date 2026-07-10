#include <IO/DiskCacheProvider.h>

#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheKey.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Common/ErrnoException.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

namespace
{

/// Object-local end of the bytes safely readable from `segment`: a fully DOWNLOADED segment is
/// readable to its inclusive `range().right`, otherwise only up to the live committed write offset.
size_t committedEnd(const FileSegment & segment)
{
    return segment.state() == FileSegmentState::DOWNLOADED
        ? segment.range().right + 1
        : segment.getCurrentWriteOffset();
}

/// pread `size` bytes at `offset_in_segment` from the cache segment file into `dst`. The holder
/// pins the segment, so a short read is a hard error.
void preadSegment(const FileSegment & segment, size_t offset_in_segment, char * dst, size_t size)
{
    ReadSettings read_settings;
    read_settings.local_fs_settings.method = LocalFSReadMethod::pread;
    read_settings.local_fs_settings.buffer_size = 0;
    auto reader = createReadBufferFromFileBase(
        segment.getPath(), read_settings, /*read_hint=*/std::nullopt, /*file_size=*/std::nullopt,
        segment.getFlagsForLocalRead());
    reader->seek(static_cast<off_t>(offset_in_segment), SEEK_SET);

    size_t copied = 0;
    while (copied < size)
    {
        reader->set(dst + copied, size - copied);
        if (!reader->next())
            break;
        const size_t got = reader->available();
        if (got == 0)
            break;
        reader->position() = reader->buffer().end();
        copied += got;
    }
    if (copied != size)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "DiskCacheProvider: short read from cache file {} at offset {}: got {}, expected {}",
            segment.getPath(), offset_in_segment, copied, size);
}

}

DiskCacheProvider::DiskCacheProvider(FileCachePtr cache_, FileCacheOriginInfo origin_, size_t boundary_alignment_)
    : cache(std::move(cache_)), origin(std::move(origin_)), boundary_alignment(boundary_alignment_)
{
}

size_t DiskCacheProvider::tryRead(const StoredObject & object, size_t offset, char * dst, size_t size)
{
    /// Cache-only probe: returns segments only if they contiguously cover the range with a
    /// downloaded prefix, never creating or filling. An empty holder is a plain miss.
    const auto key = FileCacheKey::fromPath(object.remote_path);
    auto holder = cache->getDownloadedContiguousOrEmpty(key, offset, size, origin.user_id);
    if (holder->empty())
        return 0;

    /// Walk the segments in order; require the whole window to be committed (full-or-miss in this
    /// slice). Any gap or short-committed segment aborts to a miss — the executor then refills it.
    size_t copied = 0;
    for (const auto & segment_ptr : *holder)
    {
        const FileSegment & segment = *segment_ptr;
        const size_t want_start = offset + copied;
        if (segment.range().left > want_start)
            return 0;   /// gap before this segment
        const size_t seg_end = committedEnd(segment);
        if (seg_end <= want_start)
            return 0;   /// nothing committed at the frontier
        const size_t chunk = std::min<size_t>(seg_end, offset + size) - want_start;
        preadSegment(segment, want_start - segment.range().left, dst + copied, chunk);
        copied += chunk;
        if (copied >= size)
            break;
    }
    return copied == size ? size : 0;
}

void DiskCacheProvider::write(const StoredObject & object, size_t offset, const char * data, size_t size)
{
    if (size == 0)
        return;

    /// Best-effort: a reservation failure, a lost downloader race or a disk error leaves the tier
    /// unpopulated. Never propagate — populating the cache must not fail the read.
    try
    {
        const auto key = FileCacheKey::fromPath(object.remote_path);
        CreateFileSegmentSettings create_settings(FileSegmentKind::Regular);
        auto holder = cache->getOrSet(
            key, offset, size, /*file_size=*/object.bytes_size, create_settings,
            /*file_segments_limit=*/0, origin, boundary_alignment);

        const size_t range_end = offset + size;
        for (const auto & segment_ptr : *holder)
        {
            FileSegment & segment = *segment_ptr;
            const auto & seg_range = segment.range();
            if (seg_range.left >= range_end || seg_range.right + 1 <= offset)
                continue;
            if (segment.isDetached())
                continue;

            segment.getOrSetDownloader();
            if (!segment.isDownloader())
                continue;

            /// Append-only from the segment's live write offset, bounded by both the segment and the
            /// data we hold. A hole (write offset outside what we carry) is skipped.
            const size_t write_offset = segment.getCurrentWriteOffset();
            const size_t write_end = std::min<size_t>(seg_range.right + 1, range_end);
            if (write_offset < offset || write_offset >= write_end)
            {
                segment.completePartAndResetDownloader();
                continue;
            }
            const size_t n = write_end - write_offset;

            std::string failure_reason;
            if (!segment.reserve(n, /*lock_wait_timeout_milliseconds=*/1000, failure_reason))
            {
                LOG_TRACE(log, "reserve failed for [{}, {}]: {}", seg_range.left, seg_range.right, failure_reason);
                segment.completePartAndResetDownloader();
                continue;
            }

            try
            {
                /// `data` covers [offset, range_end); the slice for this segment starts at write_offset.
                segment.write(const_cast<char *>(data) + (write_offset - offset), n, write_offset);
            }
            catch (const ErrnoException & e)
            {
                /// Disk full / IO error: leave the tier unpopulated (the read already has the bytes).
                LOG_TRACE(log, "cache write skipped for [{}, {}]: {}", seg_range.left, seg_range.right, e.displayText());
                segment.completePartAndResetDownloader();
                continue;
            }
            segment.completePartAndResetDownloader();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "DiskCacheProvider::write");
    }
}

}
