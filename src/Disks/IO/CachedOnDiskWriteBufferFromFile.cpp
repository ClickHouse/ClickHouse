#include "CachedOnDiskWriteBufferFromFile.h"

#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Common/logger_useful.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/Context.h>


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

namespace
{
    class SwapHelper
    {
    public:
        SwapHelper(WriteBuffer & b1_, WriteBuffer & b2_) : b1(b1_), b2(b2_) { b1.swap(b2); }
        ~SwapHelper() { b1.swap(b2); }

    private:
        WriteBuffer & b1;
        WriteBuffer & b2;
    };
}


FileSegmentRangeWriter::FileSegmentRangeWriter(
    FileCache * cache_,
    const FileSegment::Key & key_,
    std::shared_ptr<FilesystemCacheLog> cache_log_,
    const String & query_id_,
    const String & source_path_)
    : cache(cache_)
    , key(key_)
    , cache_log(cache_log_)
    , query_id(query_id_)
    , source_path(source_path_)
    , current_file_segment_it(file_segments_holder.file_segments.end())
{
}

bool FileSegmentRangeWriter::write(const char * data, size_t size, size_t offset, FileSegmentKind segment_kind)
{
    size_t written_size = tryWrite(data, size, offset, segment_kind, true);
    return written_size == size;
}

size_t FileSegmentRangeWriter::tryWrite(const char * data, size_t size, size_t offset, FileSegmentKind segment_kind, bool strict)
{
    size_t total_written_size = 0;
    while (size > 0)
    {
        size_t written_size = tryWriteImpl(data, size, offset, segment_kind, strict);
        chassert(written_size <= size);
        if (written_size == 0)
            break;

        if (data)
            data += written_size;

        size -= written_size;
        offset += written_size;
        total_written_size += written_size;
    }
    return total_written_size;
}

size_t FileSegmentRangeWriter::tryWriteImpl(const char * data, size_t size, size_t offset, FileSegmentKind segment_kind, bool strict)
{
    if (finalized)
        return 0;

    auto & file_segments = file_segments_holder.file_segments;

    if (current_file_segment_it == file_segments.end())
    {
        current_file_segment_it = allocateFileSegment(current_file_segment_write_offset, segment_kind);
    }
    else
    {
        auto file_segment = *current_file_segment_it;
        assert(file_segment->getCurrentWriteOffset() == current_file_segment_write_offset);

        if (current_file_segment_write_offset != offset)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot write file segment at offset {}, because current write offset is: {}",
                offset, current_file_segment_write_offset);
        }

        if (file_segment->range().size() == file_segment->getDownloadedSize())
        {
            completeFileSegment(*file_segment);
            current_file_segment_it = allocateFileSegment(current_file_segment_write_offset, segment_kind);
        }
    }

    auto & file_segment = *current_file_segment_it;

    auto downloader = file_segment->getOrSetDownloader();
    if (downloader != FileSegment::getCallerId())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set a downloader. ({})", file_segment->getInfoForLog());

    SCOPE_EXIT({
        if (file_segment->isDownloader())
            file_segment->completePartAndResetDownloader();
    });

    size_t reserved_size = file_segment->tryReserve(size, strict);
    if (reserved_size == 0 || (strict && reserved_size != size))
    {
        if (strict)
        {
            file_segment->completeWithState(FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
            appendFilesystemCacheLog(*file_segment);
        }

        LOG_DEBUG(
            &Poco::Logger::get("FileSegmentRangeWriter"),
            "Unsuccessful space reservation attempt (size: {}, file segment info: {}",
            size, file_segment->getInfoForLog());

        return 0;
    }

    /// Shrink to reserved size, because we can't write more than reserved
    size = reserved_size;

    try
    {
        file_segment->write(data, size, offset);
    }
    catch (...)
    {
        file_segment->completePartAndResetDownloader();
        throw;
    }

    file_segment->completePartAndResetDownloader();
    current_file_segment_write_offset += size;

    return size;
}

bool FileSegmentRangeWriter::reserve(size_t size, size_t offset)
{
    return write(nullptr, size, offset, FileSegmentKind::Temporary);
}

size_t FileSegmentRangeWriter::tryReserve(size_t size, size_t offset)
{
    return tryWrite(nullptr, size, offset, FileSegmentKind::Temporary);
}

void FileSegmentRangeWriter::finalize()
{
    if (finalized)
        return;

    auto & file_segments = file_segments_holder.file_segments;

    if (file_segments.empty() || current_file_segment_it == file_segments.end())
        return;

    completeFileSegment(**current_file_segment_it);
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

FileSegments::iterator FileSegmentRangeWriter::allocateFileSegment(size_t offset, FileSegmentKind segment_kind)
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

    return file_segments_holder.add(std::move(file_segment));
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

void FileSegmentRangeWriter::completeFileSegment(FileSegment & file_segment, std::optional<FileSegment::State> state)
{
    /// File segment can be detached if space reservation failed.
    if (file_segment.isDetached())
        return;

    if (state.has_value())
        file_segment.setDownloadState(*state);

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
    cacheData(working_buffer.begin(), size);
    current_download_offset += size;
}

void CachedOnDiskWriteBufferFromFile::cacheData(char * data, size_t size)
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

        tryLogCurrentException(__PRETTY_FUNCTION__);
        return;
    }
    catch (...)
    {
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
