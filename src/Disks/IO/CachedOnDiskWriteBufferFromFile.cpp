#include "CachedOnDiskWriteBufferFromFile.h"

#include <Common/FileCacheFactory.h>
#include <Common/FileSegment.h>
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
    , cache_log(Context::getGlobalContextInstance()->getFilesystemCacheLog())
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
    if (stop_caching)
        return;

    if (!cache_writer)
    {
        cache_writer = std::make_unique<FileSegmentRangeWriter>(
            cache.get(), key, [this](const FileSegment & file_segment) { appendFilesystemCacheLog(file_segment); });
    }

    Stopwatch watch(CLOCK_MONOTONIC);

    try
    {
        if (!cache_writer->write(data, size, current_download_offset, is_persistent_cache_file))
        {
            LOG_INFO(log, "Write-through cache is stopped as cache limit is reached and nothing can be evicted");

            /// No space left, disable caching.
            stop_caching = true;
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
}

void CachedOnDiskWriteBufferFromFile::appendFilesystemCacheLog(const FileSegment & file_segment)
{
    if (cache_log)
    {
        auto file_segment_range = file_segment.range();
        FilesystemCacheLogElement elem
        {
            .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
            .query_id = query_id,
            .source_file_path = source_path,
            .file_segment_range = { file_segment_range.left, file_segment_range.right },
            .requested_range = {},
            .cache_type = FilesystemCacheLogElement::CacheType::WRITE_THROUGH_CACHE,
            .file_segment_size = file_segment_range.size(),
            .cache_attempted = false,
            .read_buffer_id = {},
            .profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(current_file_segment_counters.getPartiallyAtomicSnapshot()),
        };

        current_file_segment_counters.reset();

        cache_log->add(elem);
    }
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
