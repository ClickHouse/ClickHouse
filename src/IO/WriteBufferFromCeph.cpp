#include "WriteBufferFromCeph.h"
#include <rados/librados.hpp>

#ifdef USE_CEPH

#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Throttler.h>

namespace ProfileEvents
{
    extern const Event RemoteWriteThrottlerBytes;
    extern const Event RemoteWriteThrottlerSleepMicroseconds;
    extern const Event WriteBufferFromCephMicroseconds;
    extern const Event WriteBufferFromCephBytes;
    extern const Event WriteBufferFromCephRequestsErrors;
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

WriteBufferFromCeph::WriteBufferFromCeph(
    const String & mon_uri_, /// ceph-monitor uri, e.g. "mon1:1234,mon2:1234,mon3:1234"
    const String & user_,
    const String & secret_,
    const String & pool_,
    const String & object_id_,
    const WriteSettings & write_settings_,
    size_t buf_size_,
    int flags_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , mon_uri(mon_uri_)
    , pool(pool_)
    , object_id(object_id_)
    , write_settings(write_settings_)
    , flags(flags_)
{
    /// Create rados cluster
    if (rados_create(&rados_c, user_.c_str()) < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot create rados object");
    rados = std::make_unique<librados::Rados>();
    librados::Rados::from_rados_t(rados_c, *rados);
    setCephConfig(*rados, "mon_host", mon_uri);
    setCephConfig(*rados, "key", secret_);
}

void WriteBufferFromCeph::initialize()
{
    if (initialized)
        return;

    /// Connect to rados rados
    if (rados->connect() < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot connect to ceph rados {}", mon_uri);

    /// Initialize io_ctx to target pool
    io_ctx = std::make_unique<librados::IoCtx>();
    if (rados->ioctx_create(pool.c_str(), *io_ctx) < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot create io_ctx for pool {}/{}", mon_uri, pool);

    initialized = true;
}

size_t WriteBufferFromCeph::writeImpl(const char * begin, size_t len)
{
    ResourceGuard rlock(write_settings.resource_link, len);
    int ec;
    try
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::WriteBufferFromCephMicroseconds);
        if (first_write)
        {
            /// O_CREATE | O_TRUNC
            ec = rados_write_full(io_ctx_c, object_id.c_str(), begin, len);
            first_write = false;
        }
        else
            ec = rados_append(io_ctx_c, object_id.c_str(), begin, len);

        if (write_settings.remote_throttler)
            write_settings.remote_throttler->add(len, ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);
    }
    catch (...)
    {
        write_settings.resource_link.accumulate(len); // We assume no resource was used in case of failure
        throw;
    }
    rlock.unlock();

    if (ec < 0)
    {
        write_settings.resource_link.accumulate(len); // We assume no resource was used in case of failure
            throw Exception(ErrorCodes::NETWORK_ERROR, "Fail to write Ceph object: mon_uri: {}, pool: {}, object_id: {}. Error: {}", mon_uri, pool, object_id, strerror(-ec));
    }

    write_settings.resource_link.adjust(len, len);
    ProfileEvents::increment(ProfileEvents::WriteBufferFromCephBytes, len);
    return len;
}

void WriteBufferFromCeph::nextImpl()
{
    if (!initialized)
        initialize();

    if (!offset())
        return;

    size_t bytes_written = 0;

    while (bytes_written != offset())
        bytes_written += writeImpl(working_buffer.begin() + bytes_written, offset() - bytes_written);
}

}

#endif
