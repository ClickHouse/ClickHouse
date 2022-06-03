#include "CachedWriteBufferFromFile.h"

#include <Common/FileCacheFactory.h>
#include <Common/FileSegment.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/Context.h>


namespace ProfileEvents
{
    extern const Event CachedWriteBufferCacheWriteBytes;
    extern const Event CachedWriteBufferCacheWriteMicroseconds;
}

namespace DB
{

CachedWriteBufferFromFile::CachedWriteBufferFromFile(
    std::unique_ptr<WriteBuffer> impl_,
    FileCachePtr cache_,
    const String & source_path_,
    const IFileCache::Key & key_,
    bool is_persistent_cache_file_,
    const String & query_id_,
    const WriteSettings & settings_)
    : WriteBufferFromFileDecorator(std::move(impl_))
    , cache(cache_)
    , source_path(source_path_)
    , key(key_)
    , is_persistent_cache_file(is_persistent_cache_file_)
    , query_id(query_id_)
    , enable_cache_log(!query_id_.empty() && settings_.enable_filesystem_cache_log)
{
}

void CachedWriteBufferFromFile::nextImpl()
{
    size_t size = offset();
    swap(*impl);

    try
    {
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

    swap(*impl);

    /// Write data to cache.
    cacheData(working_buffer.begin(), size);
    current_download_offset += size;
}

void CachedWriteBufferFromFile::cacheData(char * data, size_t size)
{
    if (stop_caching)
        return;

    if (!cache_writer)
    {
        cache_writer = std::make_unique<FileSegmentRangeWriter>(
            cache.get(), key, [this](const FileSegmentPtr & file_segment) { appendFilesystemCacheLog(file_segment); });
    }

    Stopwatch watch(CLOCK_MONOTONIC);

    bool cached;
    try
    {
        cached = cache_writer->write(data, size, current_download_offset, is_persistent_cache_file);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return;
    }

    if (!cached)
    {
        /// No space left, disable caching.
        stop_caching = true;
        return;
    }

    ProfileEvents::increment(ProfileEvents::CachedWriteBufferCacheWriteBytes, size);
    ProfileEvents::increment(ProfileEvents::CachedWriteBufferCacheWriteMicroseconds, watch.elapsedMicroseconds());
}

void CachedWriteBufferFromFile::appendFilesystemCacheLog(const FileSegmentPtr & file_segment)
{
    auto file_segment_range = file_segment->range();
    FilesystemCacheLogElement elem
    {
        .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
        .query_id = query_id,
        .source_file_path = source_path,
        .file_segment_range = { file_segment_range.left, file_segment_range.right },
        .requested_range = {},
        .read_type = FilesystemCacheLogElement::ReadType::READ_FROM_FS_AND_DOWNLOADED_TO_CACHE,
        .file_segment_size = file_segment_range.size(),
        .cache_attempted = false,
        .read_buffer_id = {},
        .profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(current_file_segment_counters.getPartiallyAtomicSnapshot()),
    };

    current_file_segment_counters.reset();

    if (auto cache_log = Context::getGlobalContextInstance()->getFilesystemCacheLog())
        cache_log->add(elem);
}

void CachedWriteBufferFromFile::preFinalize()
{
    if (cache_writer)
        cache_writer->finalize();
}

}
