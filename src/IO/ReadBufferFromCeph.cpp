#include "ReadBufferFromCeph.h"
#include <memory>
#include <rados/librados.hpp>

#if USE_CEPH
#include "Common/ElapsedTimeProfileEventIncrement.h"
#include "Common/Scheduler/ResourceGuard.h"
#include <Common/Throttler.h>
#include <Common/safe_cast.h>
#include <Common/Exception.h>

namespace ProfileEvents
{
    extern const Event ReadBufferFromCephMicroseconds;
    extern const Event ReadBufferFromCephBytes;
    extern const Event ReadBufferFromCephRequestsErrors;
    extern const Event ReadBufferFromCephInitMicroseconds;
    extern const Event RemoteReadThrottlerBytes;
    extern const Event RemoteReadThrottlerSleepMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CEPH_ERROR;
    extern const int NETWORK_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

static void setCephConfig(librados::Rados & rados, const String & name, const String & value)
{
    if (rados.conf_set(name.c_str(), value.c_str()) < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot set ceph config {}:{}", name, value);
}

ReadBufferFromCeph::ReadBufferFromCeph(
    const String & mon_uri_, /// ceph-monitor uri, e.g. "mon1:1234"
    const String & user_,
    const String & secret_,
    const String & pool_,
    const String & object_id_,
    const ReadSettings & read_settings_,
    bool use_external_buffer_,
    bool restricted_seek_,
    size_t read_until_position_,
    std::optional<size_t> file_size_)
    : ReadBufferFromFileBase(use_external_buffer_ ? 0 : read_settings_.remote_fs_buffer_size, nullptr, 0, file_size_)
    , mon_uri(mon_uri_)
    , pool(pool_)
    , object_id(object_id_)
    , read_settings(read_settings_)
    , tmp_buffer_size(read_settings.remote_fs_buffer_size)
    , use_external_buffer(use_external_buffer_)
    , restricted_seek(restricted_seek_)
    , read_until_position(read_until_position_)
{
    if (!use_external_buffer)
    {
        tmp_buffer.resize(tmp_buffer_size);
        data_ptr = tmp_buffer.data();
        data_capacity = tmp_buffer_size;
    }

    /// Create rados cluster
    if (rados_create(&rados_c, user_.c_str()) < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot create rados object");
    rados = std::make_unique<librados::Rados>();
    librados::Rados::from_rados_t(rados_c, *rados);
    setCephConfig(*rados, "mon_host", mon_uri);
    setCephConfig(*rados, "key", secret_);
}

void ReadBufferFromCeph::initialize()
{
    if (initialized)
        return;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadBufferFromCephInitMicroseconds);

    /// Connect to rados cluster
    if (rados->connect() < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot connect to ceph rados {}", mon_uri);

    /// Initialize io_ctx to target pool
    io_ctx = std::make_unique<librados::IoCtx>();
    if (rados->ioctx_create(pool.c_str(), *io_ctx) < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot create io_ctx for pool {}/{}", mon_uri, pool);

    initialized = true;
}

void ReadBufferFromCeph::setReadUntilEnd()
{
    if (read_until_position)
    {
        read_until_position = 0;
        if (initialized)
        {
            offset = getPosition();
            resetWorkingBuffer();
            initialized = false;
        }
    }
}

void ReadBufferFromCeph::setReadUntilPosition(size_t position)
{
    read_until_position = position;
    initialized = false;
}

size_t ReadBufferFromCeph::readImpl(char * to, size_t len, off_t begin) const
{
    ResourceGuard rlock(read_settings.resource_link, len);
    int bytes_read = 0;
    try
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadBufferFromCephMicroseconds);
        bytes_read = rados_read(io_ctx_c, object_id.c_str(), to, safe_cast<int>(len), begin);
        if (read_settings.remote_throttler)
            read_settings.remote_throttler->add(bytes_read, ProfileEvents::RemoteReadThrottlerBytes, ProfileEvents::RemoteReadThrottlerSleepMicroseconds);
    }
    catch(...)
    {
        read_settings.resource_link.accumulate(len); // We assume no resource was used in case of failure
        throw;
    }

    if (bytes_read < 0)
    {
        read_settings.resource_link.accumulate(len); // We assume no resource was used in case of failure
        throw Exception(ErrorCodes::NETWORK_ERROR, "Fail to read from Ceph: mon_uri: {}, pool: {}, object: {}. Error: {}", mon_uri, pool, object_id, strerror(-bytes_read));
    }

    read_settings.resource_link.adjust(len, bytes_read);
    ProfileEvents::increment(ProfileEvents::ReadBufferFromCephBytes, bytes_read);
    return bytes_read;
}

bool ReadBufferFromCeph::nextImpl()
{
    if (read_until_position)
    {
        if (read_until_position == offset)
            return false;

        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);
    }

    if (!initialized)
        initialize();

    if (use_external_buffer)
    {
        data_ptr = internal_buffer.begin();
        data_capacity = internal_buffer.size();
    }

    size_t num_bytes_to_read = std::min(static_cast<size_t>(total_size - offset), data_capacity);
    auto bytes_read = readImpl(data_ptr, num_bytes_to_read, offset);

    if (bytes_read == 0)
        return false;

    BufferBase::set(data_ptr, bytes_read, 0);
    offset += bytes_read;

    return true;
}

off_t ReadBufferFromCeph::seek(off_t offset_, int whence)
{
    if (offset_ == getPosition() && whence == SEEK_SET)
        return offset_;

    if (initialized && restricted_seek)
    {
        throw Exception(
            ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
            "Seek is allowed only before first read attempt from the buffer (current offset: "
            "{}, new offset: {}, reading until position: {}, available: {})",
            getPosition(), offset_, read_until_position, available());
    }

    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset_);

    if (!restricted_seek)
    {
        if (!working_buffer.empty()
            && static_cast<size_t>(offset_) >= offset - working_buffer.size()
            && offset_ < offset)
        {
            pos = working_buffer.end() - (offset - offset_);
            assert(pos >= working_buffer.begin());
            assert(pos < working_buffer.end());

            return getPosition();
        }

        off_t position = getPosition();
        if (initialized && offset_ > position)
        {
            size_t diff = offset_ - position;
            if (diff < read_settings.remote_read_min_bytes_for_seek)
            {
                ignore(diff);
                return offset_;
            }
        }

        resetWorkingBuffer();
        if (initialized)
            initialized = false;
    }

    offset = offset_;
    return offset;
}

size_t ReadBufferFromCeph::getFileSize()
{
    if (file_size)
        return *file_size;
    size_t psize;
    if (io_ctx->stat2(object_id, &psize, nullptr) < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot get size, mon_uri: {}, pool {} object {}", mon_uri, pool, object_id);
    file_size = psize;
    return psize;
}

off_t ReadBufferFromCeph::getPosition()
{
    return offset - available();
}

size_t ReadBufferFromCeph::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & /*progress_callback*/) const
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

}

#endif
