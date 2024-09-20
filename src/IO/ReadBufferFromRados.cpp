#include "ReadBufferFromRados.h"
#include "IO/BufferBase.h"

#if USE_CEPH

#include <memory>
#include <optional>
#include <IO/Ceph/RadosIOContext.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Throttler.h>
#include <Common/logger_useful.h>
#include <Common/safe_cast.h>

namespace ProfileEvents
{
extern const Event ReadBufferFromRadosMicroseconds;
extern const Event ReadBufferFromRadosBytes;
extern const Event RemoteReadThrottlerBytes;
extern const Event RemoteReadThrottlerSleepMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int SEEK_POSITION_OUT_OF_BOUND;
}

ReadBufferFromRados::ReadBufferFromRados(
    std::shared_ptr<librados::Rados> rados_,
    const String & pool,
    const String & nspace,
    const String & object_id_,
    const ReadSettings & read_settings_,
    bool use_external_buffer_,
    size_t offset_,
    size_t read_until_position_,
    bool restricted_seek_,
    std::optional<size_t> file_size_)
    : ReadBufferFromFileBase(use_external_buffer_ ? 0 : read_settings_.remote_fs_buffer_size, nullptr, 0, file_size_)
    , object_id(object_id_)
    , read_settings(read_settings_)
    , file_offset(offset_)
    , read_until_position(read_until_position_)
    , use_external_buffer(use_external_buffer_)
    , restricted_seek(restricted_seek_)
{
    auto io = std::make_shared<RadosIOContext>(rados_, pool, nspace);
}

ReadBufferFromRados::ReadBufferFromRados(
    std::shared_ptr<RadosIOContext> io_,
    const String & object_id_,
    const ReadSettings & read_settings_,
    bool use_external_buffer_,
    size_t offset_,
    size_t read_until_position_,
    bool restricted_seek_,
    std::optional<size_t> file_size_)
    : ReadBufferFromFileBase(use_external_buffer_ ? 0 : read_settings_.remote_fs_buffer_size, nullptr, 0, file_size_)
    , io_ctx(std::move(io_))
    , object_id(object_id_)
    , read_settings(read_settings_)
    , file_offset(offset_)
    , read_until_position(read_until_position_)
    , use_external_buffer(use_external_buffer_)
    , restricted_seek(restricted_seek_)
{
    io_ctx->connect();
}

ReadBufferFromRados::~ReadBufferFromRados() = default;

void ReadBufferFromRados::setReadUntilPosition(size_t position)
{
    read_until_position = position;
    resetWorkingBuffer();
}

void ReadBufferFromRados::setReadUntilEnd()
{
    if (read_until_position)
    {
        read_until_position = 0;
        resetWorkingBuffer();
    }
}

bool ReadBufferFromRados::nextImpl()
{
    size_t num_bytes_to_read;
    if (read_until_position)
    {
        if (read_until_position == file_offset)
        {
            return false;
        }

        if (read_until_position < file_offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", file_offset, read_until_position - 1);

        num_bytes_to_read = std::min<size_t>(read_until_position - file_offset, internal_buffer.size());
    }
    else
    {
        num_bytes_to_read = internal_buffer.size();
    }

    auto bytes_read = readImpl(internal_buffer.begin(), num_bytes_to_read, file_offset);

    if (bytes_read)
    {
        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);
        file_offset += bytes_read;
        return true;
    }

    return false;
}

off_t ReadBufferFromRados::seek(off_t offset_, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset_);

    if (!restricted_seek)
    {
        if (!working_buffer.empty() && size_t(offset_) >= file_offset - working_buffer.size() && offset_ < file_offset)
        {
            pos = working_buffer.end() - (file_offset - offset_);
            assert(pos >= working_buffer.begin());
            assert(pos <= working_buffer.end());

            return getPosition();
        }

        off_t position = getPosition();
        if (offset_ > position)
        {
            size_t diff = offset_ - position;
            if (diff < read_settings.remote_read_min_bytes_for_seek)
            {
                ignore(diff);
                return offset_;
            }
        }
    }

    resetWorkingBuffer();
    file_offset = offset_;
    return file_offset;
}

std::optional<size_t> ReadBufferFromRados::tryGetFileSize()
{
    if (file_size)
        return file_size;
    size_t psize;
    io_ctx->stat(object_id, &psize, nullptr);
    file_size = psize;
    return file_size;
}

off_t ReadBufferFromRados::getPosition()
{
    return file_offset - available();
}

size_t ReadBufferFromRados::getFileOffsetOfBufferEnd() const
{
    return file_offset;
}

String ReadBufferFromRados::getFileName() const
{
    return object_id;
}

size_t
ReadBufferFromRados::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & /*progress_callback*/) const
{
    size_t total_read = 0;
    while (n > 0)
    {
        auto bytes_copied = readImpl(to, n, range_begin);
        if (bytes_copied == 0)
            break;
        total_read += bytes_copied;
        range_begin += bytes_copied;
        to += bytes_copied;
        n -= bytes_copied;
    }
    return total_read;
}

size_t ReadBufferFromRados::readImpl(char * to, size_t len, off_t begin) const
{
    ResourceGuard rlock(ResourceGuard::Metrics::getIORead(), read_settings.io_scheduling.read_resource_link, len);
    size_t bytes_read = 0;
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadBufferFromRadosMicroseconds);

    bytes_read = io_ctx->read(object_id, to, len, begin);
    if (read_settings.remote_throttler && bytes_read)
        read_settings.remote_throttler->add(
            bytes_read, ProfileEvents::RemoteReadThrottlerBytes, ProfileEvents::RemoteReadThrottlerSleepMicroseconds);

    ProfileEvents::increment(ProfileEvents::ReadBufferFromRadosBytes, bytes_read);
    return bytes_read;
}
}

#endif
