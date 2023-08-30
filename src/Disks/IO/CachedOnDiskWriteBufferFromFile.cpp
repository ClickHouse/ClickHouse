#include "CachedOnDiskWriteBufferFromFile.h"

#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Common/logger_useful.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/Context.h>
#include <IO/SwapHelper.h>


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
}

FileSegmentRangeWriter::FileSegmentRangeWriter(
    FileCache * cache_,
    const FileSegment::Key & key_,
    std::shared_ptr<FilesystemCacheLog> cache_log_,
    const String & query_id_,
    const String & source_path_)
    : cache(cache_)
    , key(key_)
    , log(&Poco::Logger::get("FileSegmentRangeWriter"))
    , cache_log(cache_log_)
    , query_id(query_id_)
    , source_path(source_path_)
{
}

bool FileSegmentRangeWriter::write(const char * data, size_t size, size_t offset, FileSegmentKind segment_kind)
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

    if (!file_segments || file_segments->empty() || file_segments->front().isDownloaded())
    {
        file_segment = &allocateFileSegment(expected_write_offset, segment_kind);
    }
    else
    {
        file_segment = &file_segments->front();
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

        if (!file_segment->isDownloader()
            && file_segment->getOrSetDownloader() != FileSegment::getCallerId())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Failed to set a downloader. ({})", file_segment->getInfoForLog());
        }

        size_t size_to_write = std::min(available_size, size);

        bool reserved = file_segment->reserve(size_to_write);
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

    CreateFileSegmentSettings create_settings(segment_kind, false);

    /// We set max_file_segment_size to be downloaded,
    /// if we have less size to write, file segment will be resized in complete() method.
    file_segments = cache->set(key, offset, cache->getMaxFileSegmentSize(), create_settings);
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
        .profile_counters = nullptr,
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

    file_segment.complete();
    appendFilesystemCacheLog(file_segment);
}


CachedOnDiskWriteBufferFromFile::CachedOnDiskWriteBufferFromFile(
    std::unique_ptr<WriteBuffer> impl_,
    FileCachePtr cache_,
    const String & source_path_,
    const FileCache::Key & key_,
    const String & query_id_,
    const WriteSettings & settings_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , log(&Poco::Logger::get("CachedOnDiskWriteBufferFromFile"))
    , cache(cache_)
    , source_path(source_path_)
    , key(key_)
    , query_id(query_id_)
    , enable_cache_log(!query_id_.empty() && settings_.enable_filesystem_cache_log)
    , throw_on_error_from_cache(settings_.throw_on_error_from_cache)
{
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
        /// If something was already written to cache, remove it.
        cache_writer.reset();
        cache->removeKeyIfExists(key);

        throw;
    }
}

void CachedOnDiskWriteBufferFromFile::cacheData(char * data, size_t size, bool throw_on_error)
{
    if (cache_in_error_state_or_disabled)
        return;

    if (!cache_writer)
    {
        std::shared_ptr<FilesystemCacheLog> cache_log;
        if (enable_cache_log)
            cache_log = Context::getGlobalContextInstance()->getFilesystemCacheLog();

        cache_writer = std::make_unique<FileSegmentRangeWriter>(cache.get(), key, cache_log, query_id, source_path);
    }

    Stopwatch watch(CLOCK_MONOTONIC);

    cache_in_error_state_or_disabled = true;

    try
    {
        if (!cache_writer->write(data, size, current_download_offset, FileSegmentKind::Regular))
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

}
