#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>

#include <Common/logger_useful.h>
#include <Common/ErrnoException.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/Context.h>
#include <IO/SwapHelper.h>
#include <IO/NullWriteBuffer.h>


namespace ProfileEvents
{
    extern const Event CachedWriteBufferCacheWriteBytes;
    extern const Event CachedWriteBufferCacheWriteMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FILECACHE_CANNOT_WRITE_THROUGH_CACHE_WITH_CONCURRENT_READS;
}

FileSegmentRangeWriter::FileSegmentRangeWriter(
    FileCache * cache_,
    const FileSegment::Key & key_,
    const FileCacheOriginInfo & origin_,
    size_t reserve_space_lock_wait_timeout_milliseconds_,
    std::shared_ptr<FilesystemCacheLog> cache_log_,
    const String & query_id_,
    const String & source_path_,
    bool is_distributed_cache_)
    : cache(cache_)
    , key(key_)
    , origin(origin_)
    , reserve_space_lock_wait_timeout_milliseconds(reserve_space_lock_wait_timeout_milliseconds_)
    , log(getLogger("FileSegmentRangeWriter"))
    , cache_log(cache_log_)
    , query_id(query_id_)
    , source_path(source_path_)
    , is_distributed_cache(is_distributed_cache_)
{
}

bool FileSegmentRangeWriter::write(char * data, size_t size, size_t offset, FileSegmentKind segment_kind)
{
    if (finalized)
        return false;

    if (expected_write_offset != offset)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot write file segment at offset {}, because expected write offset is: {}",
            offset, expected_write_offset);
    }

    FileSegment * file_segment;

    if (!ignore_bytes
        && (!file_segments || file_segments->empty() || file_segments->front().isDownloaded()))
    {
        file_segment = &allocateFileSegment(expected_write_offset, segment_kind);

        if (is_distributed_cache && file_segment->getCurrentWriteOffset() > offset)
        {
            /// We could have allowed this only in case jumpToPosition was called to non-zero value,
            /// and this would be correct for MergeTree*, but, there are cases, like disk->checkAccess methods,
            /// where this method can be called twice for the same server,
            /// while path contains server uuid, e.g. the cache key will be the same,
            /// so file segment here can have downloaded_size > 0.
            ignore_bytes = file_segment->range().right - offset + 1;
            LOG_TEST(log, "Will ignore {} bytes from file segment {}", ignore_bytes, file_segment->getInfoForLog());
        }
    }
    else
    {
        file_segment = &file_segments->front();
    }

    if (ignore_bytes)
    {
        LOG_TEST(log, "Ignore bytes: {}, current size: {}", ignore_bytes, size);
        if (ignore_bytes >= size)
        {
            expected_write_offset += size;
            ignore_bytes -= size;
            return true;
        }
        size -= ignore_bytes;
        data += ignore_bytes;
        offset += ignore_bytes;
        expected_write_offset += ignore_bytes;

        file_segment = &allocateFileSegment(expected_write_offset, segment_kind);
    }

    SCOPE_EXIT({
        if (!file_segments || file_segments->empty())
            return;
        if (file_segments->front().isDownloader())
            file_segments->front().completePartAndResetDownloader();
    });

    while (size > 0)
    {
        size_t available_size = file_segment->range().size() - file_segment->getDownloadedSize();
        if (available_size == 0)
        {
            completeFileSegment();
            file_segment = &allocateFileSegment(expected_write_offset, segment_kind);
            continue;
        }

        if (!file_segment->isDownloader())
        {
            if (file_segment->getOrSetDownloader() != FileSegment::getCallerId())
            {
                /// As processing of write to distributed cache can be a bit delayed,
                /// it is not guaranteed that concurrent SELECT is not able to set downloader before us.
                throw Exception(
                    is_distributed_cache
                    ? ErrorCodes::FILECACHE_CANNOT_WRITE_THROUGH_CACHE_WITH_CONCURRENT_READS
                    : ErrorCodes::LOGICAL_ERROR,
                    "Failed to set a downloader. ({})", file_segment->getInfoForLog());
            }

            if (file_segment->getCurrentWriteOffset() > offset)
            {
                /// As processing of write to distributed cache can be a bit delayed,
                /// it is not guaranteed that concurrent SELECT did not download the file segment ahead of us.
                throw Exception(
                    is_distributed_cache
                    ? ErrorCodes::FILECACHE_CANNOT_WRITE_THROUGH_CACHE_WITH_CONCURRENT_READS
                    : ErrorCodes::LOGICAL_ERROR,
                    "Offset {} is outdated. ({})", offset, file_segment->getInfoForLog());
            }
        }
        size_t size_to_write = std::min(available_size, size);

        std::string failure_reason;
        bool reserved = file_segment->reserve(size_to_write, reserve_space_lock_wait_timeout_milliseconds, failure_reason);
        if (!reserved)
        {
            appendFilesystemCacheLog(*file_segment);

            LOG_DEBUG(
                log, "Failed to reserve space in cache (size: {}, file segment info: {}",
                size, file_segment->getInfoForLog());

            return false;
        }

        file_segment->write(data, size_to_write, offset);
        file_segment->completePartAndResetDownloader();

        size -= size_to_write;
        expected_write_offset += size_to_write;
        offset += size_to_write;
        data += size_to_write;
    }

    size_t available_size = file_segment->range().size() - file_segment->getDownloadedSize();
    if (available_size == 0)
        completeFileSegment();

    return true;
}

void FileSegmentRangeWriter::finalize()
{
    if (finalized)
        return;

    completeFileSegment();
    finalized = true;
}

FileSegmentRangeWriter::~FileSegmentRangeWriter()
{
    try
    {
        if (!finalized)
            finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

FileSegment & FileSegmentRangeWriter::allocateFileSegment(size_t offset, FileSegmentKind segment_kind)
{
    /**
    * Allocate a new file segment starting `offset`.
    * File segment capacity will equal `max_file_segment_size`, but actual size is 0.
    */

    CreateFileSegmentSettings create_settings(segment_kind);

    /// We set max_file_segment_size to be downloaded,
    /// if we have less size to write, file segment will be resized in complete() method.
    if (is_distributed_cache)
    {
        file_segments = cache->getOrSet(
            key, offset, /* size */cache->getMaxFileSegmentSize(),
            /* file_size */0, create_settings, /* file_segments_limit */1, origin);

        const auto & file_segment = file_segments->front();
        if (file_segment.getDownloadedSize() != 0)
        {
            LOG_TRACE(
                log, "File segment already exists and has downloaded size ({}) "
                "(write offset: {}, file segment range: {}), "
                "current write offset is {}. Will continue download offset",
                file_segment.getDownloadedSize(),
                file_segment.getCurrentWriteOffset(),
                file_segment.range().toString(), offset);
        }

        if (file_segment.getCurrentWriteOffset() > offset
            && file_segment.isBackgroundDownloadEnabled())
        {
            LOG_TRACE(log, "Writing at offset {}, but covering file segment has write offset {}. "
                      "This could be because background download is turned on",
                      offset, file_segment.getCurrentWriteOffset());
        }
        else if (file_segment.getCurrentWriteOffset() != offset)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Writing at offset {}, but covering file segment has write offset {} ({})",
                offset, file_segment.getCurrentWriteOffset(),
                file_segment.getInfoForLog());
        }
    }
    else
    {
        file_segments = cache->set(key, offset, cache->getMaxFileSegmentSize(), create_settings, origin);
    }

    chassert(file_segments->size() == 1);
    return file_segments->front();
}

void FileSegmentRangeWriter::appendFilesystemCacheLog(const FileSegment & file_segment)
{
    if (!cache_log)
        return;

    auto file_segment_range = file_segment.range();
    size_t file_segment_right_bound = file_segment_range.left + file_segment.getDownloadedSize() - 1;

    FilesystemCacheLogElement elem
    {
        .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
        .query_id = query_id,
        .source_file_path = source_path,
        .file_segment_range = { file_segment_range.left, file_segment_right_bound },
        .requested_range = {},
        .cache_type = FilesystemCacheLogElement::CacheType::WRITE_THROUGH_CACHE,
        .file_segment_key = {},
        .file_segment_size = file_segment_range.size(),
        .read_from_cache_attempted = false,
        .read_buffer_id = {},
    };

    cache_log->add(std::move(elem));
}

void FileSegmentRangeWriter::completeFileSegment()
{
    if (!file_segments || file_segments->empty())
        return;

    auto & file_segment = file_segments->front();
    /// File segment can be detached if space reservation failed.
    if (file_segment.isDetached() || file_segment.isCompleted())
        return;

    LOG_TEST(log, "Completing file segment {}:{}", file_segment.key(), file_segment.offset());

    /// We do not force shrink file segment in case of distributed cache,
    /// because it is possible that we reconnected and
    /// used a different connection to continue writing to cache,
    /// so we want to continue writing to existing file segment.
    /// The drawback - we do not know when to actually shrink it,
    /// but in fact it does not affect anything,
    /// file segment size != reserved size.
    /// TODO: we could send a packet from client indicating end of file.
    file_segments->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/!is_distributed_cache);
    appendFilesystemCacheLog(file_segment);
}

void FileSegmentRangeWriter::jumpToPosition(size_t position)
{
    if (!position)
        return;

    if (file_segments && !file_segments->empty())
    {
        auto & file_segment = file_segments->front();

        const auto current_write_offset = file_segment.getCurrentWriteOffset();
        if (position < current_write_offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot jump backwards: {} < {}", position, current_write_offset);

        file_segments->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/!is_distributed_cache);
        file_segments = nullptr;
    }

    expected_write_offset = position;
}

CachedOnDiskWriteBufferFromFile::CachedOnDiskWriteBufferFromFile(
    std::unique_ptr<WriteBuffer> impl_,
    FileCachePtr cache_,
    const String & source_path_,
    const FileCache::Key & key_,
    const String & query_id_,
    const WriteSettings & settings_,
    const FileCacheOriginInfo & origin_,
    std::shared_ptr<FilesystemCacheLog> cache_log_,
    bool is_distributed_cache_,
    FileSegmentKind file_segment_kind_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , log(getLogger("CachedOnDiskWriteBufferFromFile"))
    , cache(cache_)
    , source_path(source_path_)
    , key(key_)
    , query_id(query_id_)
    , origin(origin_)
    , reserve_space_lock_wait_timeout_milliseconds(settings_.filesystem_cache_reserve_space_wait_lock_timeout_milliseconds)
    , throw_on_error_from_cache(settings_.throw_on_error_from_cache)
    , is_distributed_cache(is_distributed_cache_)
    , file_segment_kind(file_segment_kind_)
    , cache_log(!query_id_.empty() && settings_.enable_filesystem_cache_log ? cache_log_ : nullptr)
{
    LOG_TEST(log, "Cache key: {}, source path: {}, is distributed cache: {}",
             key.toString(), source_path, is_distributed_cache);
}

void CachedOnDiskWriteBufferFromFile::nextImpl()
{
    size_t size = offset();

    /// Write data to cache.
    cacheData(working_buffer.begin(), size, throw_on_error_from_cache);
    current_download_offset += size;

    try
    {
        SwapHelper swap(*this, *impl);
        /// Write data to the underlying buffer.
        /// Actually here WriteBufferFromFileDecorator::nextImpl has to be called, but it is pivate method.
        /// In particular WriteBufferFromFileDecorator introduces logic with swaps in order to achieve delegation.
        impl->next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        /// If something was already written to cache, remove it.
        cache_writer.reset();
        cache->removeKeyIfExists(key, origin.user_id);

        throw;
    }
}

void CachedOnDiskWriteBufferFromFile::cacheData(char * data, size_t size, bool throw_on_error)
{
    if (cache_in_error_state_or_disabled)
        return;

    if (!cache_writer)
    {
        cache_writer = std::make_unique<FileSegmentRangeWriter>(
            cache.get(), key, origin, reserve_space_lock_wait_timeout_milliseconds,
            cache_log, query_id, source_path, is_distributed_cache);

        if (cache_writer_start_position)
            cache_writer->jumpToPosition(cache_writer_start_position);
    }

    Stopwatch watch(CLOCK_MONOTONIC);

    cache_in_error_state_or_disabled = true;

    try
    {
        if (!cache_writer->write(data, size, current_download_offset, file_segment_kind))
        {
            LOG_INFO(log, "Write-through cache is stopped as cache limit is reached and nothing can be evicted");
            return;
        }
    }
    catch (ErrnoException & e)
    {
        int code = e.getErrno();
        if (code == /* No space left on device */28 || code == /* Quota exceeded */122)
        {
            LOG_INFO(log, "Insert into cache is skipped due to insufficient disk space. ({})", e.displayText());
            return;
        }

        if (throw_on_error)
            throw;

        tryLogCurrentException(__PRETTY_FUNCTION__);
        return;
    }
    catch (...)
    {
        if (throw_on_error)
            throw;

        tryLogCurrentException(__PRETTY_FUNCTION__);
        return;
    }

    ProfileEvents::increment(ProfileEvents::CachedWriteBufferCacheWriteBytes, size);
    ProfileEvents::increment(ProfileEvents::CachedWriteBufferCacheWriteMicroseconds, watch.elapsedMicroseconds());

    cache_in_error_state_or_disabled = false;
}

void CachedOnDiskWriteBufferFromFile::finalizeImpl()
{
    try
    {
        WriteBufferFromFileDecorator::finalizeImpl();
    }
    catch (...)
    {
        if (cache_writer)
        {
            try
            {
                cache_writer->finalize();
                cache_writer.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        throw;
    }

    if (cache_writer)
    {
        cache_writer->finalize();
        cache_writer.reset();
    }
}

void CachedOnDiskWriteBufferFromFile::jumpToPosition(size_t position)
{
    chassert(is_distributed_cache);
    if (!dynamic_cast<const NullWriteBufferWithMemory *>(impl.get()))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Jumping to position in CachedOnDiskWriteBufferFromFile "
                        "is allowed only for NullWriteBufferWithMemory");
    }

    if (!position)
        return;

    if (cache_writer)
        cache_writer->jumpToPosition(position);
    else
        cache_writer_start_position = position;

    current_download_offset = position;

    LOG_TEST(log, "Jumped to position: {}", position);
}

}
