#include <Disks/IO/ReadBufferFromRemoteFSGather.h>

#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Interpreters/Cache/FileCache.h>
#include <IO/CachedInMemoryReadBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/SwapHelper.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Common/logger_useful.h>

using namespace DB;

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
    bool use_external_buffer_,
    size_t buffer_size)
    : ReadBufferFromFileBase(use_external_buffer_ ? 0 : buffer_size, nullptr, 0)
    , settings(settings_)
    , blobs_to_read(blobs_to_read_)
    , read_buffer_creator(std::move(read_buffer_creator_))
    , query_id(CurrentThread::getQueryId())
    , use_external_buffer(use_external_buffer_)
    , with_file_cache(settings.enable_filesystem_cache)
    , log(getLogger("ReadBufferFromRemoteFSGather"))
{
    if (!blobs_to_read.empty())
        current_object = blobs_to_read.front();
}

SeekableReadBufferPtr ReadBufferFromRemoteFSGather::createImplementationBuffer(const StoredObject & object, size_t start_offset)
{
    current_object = object;
    auto buf = read_buffer_creator(/* restricted_seek */true, object);

    if (read_until_position > start_offset && read_until_position < start_offset + object.bytes_size)
        buf->setReadUntilPosition(read_until_position - start_offset);

    return buf;
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
        for (size_t i = 0; i < blobs_to_read.size(); ++i)
        {
            const auto & blob = blobs_to_read[i];
            if (i == current_buf_idx)
            {
                if (offset + size <= blob.bytes_size)
                    return current_buf->isContentCached(offset, size);
                return false;
            }
            if (offset < blob.bytes_size)
                return false;
            offset -= blob.bytes_size;
        }
    }

    return false;
}
}
