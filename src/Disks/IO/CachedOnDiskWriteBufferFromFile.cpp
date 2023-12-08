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

    auto & file_segments = file_segments_holder.file_segments;

    if (file_segments.empty() || file_segments.back()->isDownloaded())
    {
        allocateFileSegment(expected_write_offset, segment_kind);
    }

    auto & file_segment = file_segments.back();

    SCOPE_EXIT({
        if (file_segments.back()->isDownloader())
            file_segments.back()->completePartAndResetDownloader();
    });

    while (size > 0)
    {
        size_t available_size = file_segment->range().size() - file_segment->getDownloadedSize();
        if (available_size == 0)
        {
            completeFileSegment(*file_segment);
            file_segment = allocateFileSegment(expected_write_offset, segment_kind);
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
            file_segment->completeWithState(FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
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

    auto & file_segments = file_segments_holder.file_segments;
    if (file_segments.empty())
        return;

    completeFileSegment(*file_segments.back());
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

FileSegmentPtr & FileSegmentRangeWriter::allocateFileSegment(size_t offset, FileSegmentKind segment_kind)
{
    /**
    * Allocate a new file segment starting `offset`.
    * File segment capacity will equal `max_file_segment_size`, but actual size is 0.
    */

    std::lock_guard cache_lock(cache->mutex);

    CreateFileSegmentSettings create_settings(segment_kind);

    /// We set max_file_segment_size to be downloaded,
    /// if we have less size to write, file segment will be resized in complete() method.
    auto file_segment = cache->createFileSegmentForDownload(
        key, offset, cache->max_file_segment_size, create_settings, cache_lock);

    auto & file_segments = file_segments_holder.file_segments;
    return *file_segments.insert(file_segments.end(), file_segment);
}

void FileSegmentRangeWriter::appendFilesystemCacheLog(const FileSegment & file_segment)
{
    if (cache_log)
    {
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
            .file_segment_size = file_segment_range.size(),
            .read_from_cache_attempted = false,
            .read_buffer_id = {},
            .profile_counters = nullptr,
        };

        cache_log->add(elem);
    }
}

void FileSegmentRangeWriter::completeFileSegment(FileSegment & file_segment)
{
    /// File segment can be detached if space reservation failed.
    if (file_segment.isDetached() || file_segment.isCompleted())
        return;

    file_segment.completeWithoutState();
    appendFilesystemCacheLog(file_segment);
}


CachedOnDiskWriteBufferFromFile::CachedOnDiskWriteBufferFromFile(
    std::unique_ptr<WriteBuffer> impl_,
    FileCachePtr cache_,
    const String & source_path_,
    const FileCache::Key & key_,
    bool is_persistent_cache_file_,
    const String & query_id_,
    const WriteSettings & settings_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , log(&Poco::Logger::get("CachedOnDiskWriteBufferFromFile"))
    , cache(cache_)
    , source_path(source_path_)
    , key(key_)
    , is_persistent_cache_file(is_persistent_cache_file_)
    , query_id(query_id_)
    , enable_cache_log(!query_id_.empty() && settings_.enable_filesystem_cache_log)
    , throw_on_error_from_cache(settings_.throw_on_error_from_cache)
{
}

void CachedOnDiskWriteBufferFromFile::nextImpl()
{
    size_t size = offset();

    try
    {
        SwapHelper swap(*this, *impl);
        /// Write data to the underlying buffer.
        impl->next();
    }
    catch (...)
    {
        /// If something was already written to cache, remove it.
        cache_writer.reset();
        cache->removeIfExists(key);

        throw;
    }

    /// Write data to cache.
    cacheData(working_buffer.begin(), size, throw_on_error_from_cache);
    current_download_offset += size;
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
        auto segment_kind = is_persistent_cache_file ? FileSegmentKind::Persistent : FileSegmentKind::Regular;
        if (!cache_writer->write(data, size, current_download_offset, segment_kind))
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
        SwapHelper swap(*this, *impl);
        impl->finalize();
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
