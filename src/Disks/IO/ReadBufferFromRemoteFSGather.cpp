#include "ReadBufferFromRemoteFSGather.h"

#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Interpreters/Cache/FileCache.h>
#include <IO/CachedInMemoryReadBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/SwapHelper.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Common/logger_useful.h>

using namespace DB;


namespace
{
    bool withFileCache(const ReadSettings & settings)
    {
        return settings.remote_fs_cache && settings.enable_filesystem_cache;
    }

    bool withPageCache(const ReadSettings & settings, bool with_file_cache)
    {
        return settings.page_cache && !with_file_cache && settings.use_page_cache_for_disks_without_file_cache;
    }
}

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

size_t chooseBufferSizeForRemoteReading(const DB::ReadSettings & settings, size_t file_size)
{
    /// Only when cache is used we could download bigger portions of FileSegments than what we actually gonna read within particular task.
    if (!withFileCache(settings))
        return settings.remote_fs_buffer_size;

    /// Buffers used for prefetch and pre-download better to have enough size, but not bigger than the whole file.
    return std::min<size_t>(std::max<size_t>(settings.remote_fs_buffer_size, DBMS_DEFAULT_BUFFER_SIZE), file_size);
}

ReadBufferFromRemoteFSGather::ReadBufferFromRemoteFSGather(
    ReadBufferCreator && read_buffer_creator_,
    const StoredObjects & blobs_to_read_,
    const std::string & cache_path_prefix_,
    const ReadSettings & settings_,
    std::shared_ptr<FilesystemCacheLog> cache_log_,
    bool use_external_buffer_)
    : ReadBufferFromFileBase(use_external_buffer_ ? 0 : chooseBufferSizeForRemoteReading(
        settings_, getTotalSize(blobs_to_read_)), nullptr, 0)
    , settings(settings_)
    , blobs_to_read(blobs_to_read_)
    , read_buffer_creator(std::move(read_buffer_creator_))
    , cache_path_prefix(cache_path_prefix_)
    , cache_log(settings.enable_filesystem_cache_log ? cache_log_ : nullptr)
    , query_id(CurrentThread::getQueryId())
    , use_external_buffer(use_external_buffer_)
    , with_file_cache(withFileCache(settings))
    , with_page_cache(withPageCache(settings, with_file_cache))
    , log(getLogger("ReadBufferFromRemoteFSGather"))
{
    if (!blobs_to_read.empty())
        current_object = blobs_to_read.front();
}

SeekableReadBufferPtr ReadBufferFromRemoteFSGather::createImplementationBuffer(const StoredObject & object, size_t start_offset)
{
    if (current_buf && !with_file_cache)
    {
        appendUncachedReadInfo();
    }

    current_object = object;
    const auto & object_path = object.remote_path;

    std::unique_ptr<ReadBufferFromFileBase> buf;

#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD
    if (with_file_cache)
    {
        auto cache_key = settings.remote_fs_cache->createKeyForPath(object_path);
        buf = std::make_unique<CachedOnDiskReadBufferFromFile>(
            object_path,
            cache_key,
            settings.remote_fs_cache,
            FileCache::getCommonUser(),
            [=, this]() { return read_buffer_creator(/* restricted_seek */true, object); },
            settings,
            query_id,
            object.bytes_size,
            /* allow_seeks */false,
            /* use_external_buffer */true,
            /* read_until_position */std::nullopt,
            cache_log);
    }
#endif

    /// Can't wrap CachedOnDiskReadBufferFromFile in CachedInMemoryReadBufferFromFile because the
    /// former doesn't support seeks.
    if (with_page_cache && !buf)
    {
        auto inner = read_buffer_creator(/* restricted_seek */false, object);
        auto cache_key = FileChunkAddress { .path = cache_path_prefix + object_path };
        buf = std::make_unique<CachedInMemoryReadBufferFromFile>(
            cache_key, settings.page_cache, std::move(inner), settings);
    }

    if (!buf)
        buf = read_buffer_creator(/* restricted_seek */true, object);

    if (read_until_position > start_offset && read_until_position < start_offset + object.bytes_size)
        buf->setReadUntilPosition(read_until_position - start_offset);

    return buf;
}

void ReadBufferFromRemoteFSGather::appendUncachedReadInfo()
{
    if (!cache_log || current_object.remote_path.empty())
        return;

    FilesystemCacheLogElement elem
    {
        .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
        .query_id = query_id,
        .source_file_path = current_object.remote_path,
        .file_segment_range = { 0, current_object.bytes_size },
        .cache_type = FilesystemCacheLogElement::CacheType::READ_FROM_FS_BYPASSING_CACHE,
        .file_segment_key = {},
        .file_segment_offset = {},
        .file_segment_size = current_object.bytes_size,
        .read_from_cache_attempted = false,
    };
    cache_log->add(std::move(elem));
}

void ReadBufferFromRemoteFSGather::initialize()
{
    if (blobs_to_read.empty())
        return;

    /// One clickhouse file can be split into multiple files in remote fs.
    size_t start_offset = 0;
    for (size_t i = 0; i < blobs_to_read.size(); ++i)
    {
        const auto & object = blobs_to_read[i];

        if (start_offset + object.bytes_size > file_offset_of_buffer_end)
        {
            LOG_TEST(log, "Reading from file: {} ({})", object.remote_path, object.local_path);

            /// Do not create a new buffer if we already have what we need.
            if (!current_buf || current_buf_idx != i)
            {
                current_buf_idx = i;
                current_buf = createImplementationBuffer(object, start_offset);
            }

            current_buf->seek(file_offset_of_buffer_end - start_offset, SEEK_SET);
            return;
        }

        start_offset += object.bytes_size;
    }
    current_buf_idx = blobs_to_read.size();
    current_buf = nullptr;
}

bool ReadBufferFromRemoteFSGather::nextImpl()
{
    /// Find first available buffer that fits to given offset.
    if (!current_buf)
        initialize();

    if (!current_buf)
        return false;

    if (readImpl())
        return true;

    if (!moveToNextBuffer())
        return false;

    return readImpl();
}

bool ReadBufferFromRemoteFSGather::moveToNextBuffer()
{
    /// If there is no available buffers - nothing to read.
    if (current_buf_idx + 1 >= blobs_to_read.size() || (read_until_position && file_offset_of_buffer_end >= read_until_position))
        return false;

    ++current_buf_idx;

    const auto & object = blobs_to_read[current_buf_idx];
    LOG_TEST(log, "Reading from next file: {} ({})", object.remote_path, object.local_path);
    current_buf = createImplementationBuffer(object, file_offset_of_buffer_end);

    return true;
}

bool ReadBufferFromRemoteFSGather::readImpl()
{
    SwapHelper swap(*this, *current_buf);

    bool result = current_buf->next();
    if (result)
    {
        file_offset_of_buffer_end += current_buf->available();
        nextimpl_working_buffer_offset = current_buf->offset();

        chassert(current_buf->available());
        chassert(blobs_to_read.size() != 1 || file_offset_of_buffer_end == current_buf->getFileOffsetOfBufferEnd());
    }

    return result;
}

void ReadBufferFromRemoteFSGather::setReadUntilPosition(size_t position)
{
    if (position == read_until_position)
        return;

    reset();
    read_until_position = position;
}

void ReadBufferFromRemoteFSGather::reset()
{
    current_object = StoredObject();
    current_buf_idx = {};
    current_buf.reset();
}

off_t ReadBufferFromRemoteFSGather::seek(off_t offset, int whence)
{
    if (offset == getPosition() && whence == SEEK_SET)
        return offset;

    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (use_external_buffer)
    {
        /// In case use_external_buffer == true, the buffer manages seeks itself.
        reset();
    }
    else
    {
        if (!working_buffer.empty()
            && static_cast<size_t>(offset) >= file_offset_of_buffer_end - working_buffer.size()
            && static_cast<size_t>(offset) < file_offset_of_buffer_end)
        {
            pos = working_buffer.end() - (file_offset_of_buffer_end - offset);
            assert(pos >= working_buffer.begin());
            assert(pos < working_buffer.end());

            return getPosition();
        }

        off_t position = getPosition();
        if (current_buf && offset > position)
        {
            size_t diff = offset - position;
            if (diff < settings.remote_read_min_bytes_for_seek)
            {
                ignore(diff);
                return offset;
            }
        }

        resetWorkingBuffer();
        reset();
    }

    file_offset_of_buffer_end = offset;
    return file_offset_of_buffer_end;
}

ReadBufferFromRemoteFSGather::~ReadBufferFromRemoteFSGather()
{
    if (!with_file_cache)
        appendUncachedReadInfo();
}

bool ReadBufferFromRemoteFSGather::isSeekCheap()
{
    return !current_buf || current_buf->isSeekCheap();
}

bool ReadBufferFromRemoteFSGather::isContentCached(size_t offset, size_t size)
{
    if (!current_buf)
        initialize();

    if (current_buf)
    {
        /// offset should be adjusted the same way as we do it in initialize()
        for (const auto & blob : blobs_to_read)
            if (offset >= blob.bytes_size)
                offset -= blob.bytes_size;

        return current_buf->isContentCached(offset, size);
    }

    return false;
}
}
