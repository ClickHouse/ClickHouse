#include "ReadBufferFromRemoteFSGather.h"

#include <IO/SeekableReadBuffer.h>

#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Common/logger_useful.h>
#include <IO/SwapHelper.h>
#include <iostream>
#include <base/hex.h>
#include <Interpreters/FilesystemCacheLog.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

ReadBufferFromRemoteFSGather::ReadBufferFromRemoteFSGather(
    ReadBufferCreator && read_buffer_creator_,
    const StoredObjects & blobs_to_read_,
    const ReadSettings & settings_,
    std::shared_ptr<FilesystemCacheLog> cache_log_)
    : ReadBufferFromFileBase(0, nullptr, 0)
    , settings(settings_)
    , blobs_to_read(blobs_to_read_)
    , read_buffer_creator(std::move(read_buffer_creator_))
    , cache_log(settings.enable_filesystem_cache_log ? cache_log_ : nullptr)
    , query_id(CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() != nullptr ? CurrentThread::getQueryId() : "")
    , log(&Poco::Logger::get("ReadBufferFromRemoteFSGather"))
{
    if (!blobs_to_read.empty())
        current_object = blobs_to_read.front();

    with_cache = settings.remote_fs_cache
        && settings.enable_filesystem_cache
        && (!query_id.empty() || settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache || !settings.avoid_readthrough_cache_outside_query_context);
}

SeekableReadBufferPtr ReadBufferFromRemoteFSGather::createImplementationBuffer(const StoredObject & object)
{
    if (current_buf && !with_cache)
    {
        appendUncachedReadInfo();
    }

    current_object = object;
    const auto & object_path = object.remote_path;

    size_t current_read_until_position = read_until_position ? read_until_position : object.bytes_size;
    auto current_read_buffer_creator = [=, this]() { return read_buffer_creator(object_path, current_read_until_position); };

    if (with_cache)
    {
        auto cache_key = settings.remote_fs_cache->createKeyForPath(object_path);
        return std::make_shared<CachedOnDiskReadBufferFromFile>(
            object_path,
            cache_key,
            settings.remote_fs_cache,
            std::move(current_read_buffer_creator),
            settings,
            query_id,
            object.bytes_size,
            /* allow_seeks */false,
            /* use_external_buffer */true,
            read_until_position ? std::optional<size_t>(read_until_position) : std::nullopt,
            cache_log);
    }

    return current_read_buffer_creator();
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
        .file_segment_size = current_object.bytes_size,
        .read_from_cache_attempted = false,
    };
    cache_log->add(elem);
}

IAsynchronousReader::Result ReadBufferFromRemoteFSGather::readInto(char * data, size_t size, size_t offset, size_t ignore)
{
    /**
     * Set `data` to current working and internal buffers.
     * Internal buffer with size `size`. Working buffer with size 0.
     */
    set(data, size);

    file_offset_of_buffer_end = offset;
    bytes_to_ignore = ignore;

    const auto result = nextImpl();

    if (result)
        return { working_buffer.size(), BufferBase::offset(), nullptr };

    return {0, 0, nullptr};
}

void ReadBufferFromRemoteFSGather::initialize()
{
    if (blobs_to_read.empty())
        return;

    /// One clickhouse file can be split into multiple files in remote fs.
    auto current_buf_offset = file_offset_of_buffer_end;
    for (size_t i = 0; i < blobs_to_read.size(); ++i)
    {
        const auto & object = blobs_to_read[i];

        if (object.bytes_size > current_buf_offset)
        {
            LOG_TEST(log, "Reading from file: {} ({})", object.remote_path, object.local_path);

            /// Do not create a new buffer if we already have what we need.
            if (!current_buf || current_buf_idx != i)
            {
                current_buf_idx = i;
                current_buf = createImplementationBuffer(object);
            }

            current_buf->seek(current_buf_offset, SEEK_SET);
            return;
        }

        current_buf_offset -= object.bytes_size;
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
    if (current_buf_idx + 1 >= blobs_to_read.size())
        return false;

    ++current_buf_idx;

    const auto & object = blobs_to_read[current_buf_idx];
    LOG_TEST(log, "Reading from next file: {} ({})", object.remote_path, object.local_path);
    current_buf = createImplementationBuffer(object);

    return true;
}

bool ReadBufferFromRemoteFSGather::readImpl()
{
    SwapHelper swap(*this, *current_buf);

    bool result = false;

    /**
     * Lazy seek is performed here.
     * In asynchronous buffer when seeking to offset in range [pos, pos + min_bytes_for_seek]
     * we save how many bytes need to be ignored (new_offset - position() bytes).
     */
    if (bytes_to_ignore)
    {
        current_buf->ignore(bytes_to_ignore);
        result = current_buf->hasPendingData();
        file_offset_of_buffer_end += bytes_to_ignore;
        bytes_to_ignore = 0;
    }

    if (!result)
        result = current_buf->next();

    if (blobs_to_read.size() == 1)
    {
        file_offset_of_buffer_end = current_buf->getFileOffsetOfBufferEnd();
    }
    else
    {
        /// For log family engines there are multiple s3 files for the same clickhouse file
        file_offset_of_buffer_end += current_buf->available();
    }

    /// Required for non-async reads.
    if (result)
    {
        assert(current_buf->available());
        nextimpl_working_buffer_offset = current_buf->offset();
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
    current_object = {};
    current_buf_idx = {};
    current_buf.reset();
    bytes_to_ignore = 0;
}

off_t ReadBufferFromRemoteFSGather::seek(off_t offset, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only seeking with SEEK_SET is allowed");

    reset();
    file_offset_of_buffer_end = offset;
    return file_offset_of_buffer_end;
}

size_t ReadBufferFromRemoteFSGather::getImplementationBufferOffset() const
{
    if (!current_buf)
        return file_offset_of_buffer_end;

    return current_buf->getFileOffsetOfBufferEnd();
}

ReadBufferFromRemoteFSGather::~ReadBufferFromRemoteFSGather()
{
    if (!with_cache)
        appendUncachedReadInfo();
}

}
