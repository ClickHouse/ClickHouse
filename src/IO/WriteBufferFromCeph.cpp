#include "WriteBufferFromCeph.h"
#include "IO/Ceph/RadosIO.h"
#include "IO/WriteBufferFromFileBase.h"

#if USE_CEPH

#include <memory>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Throttler.h>
#include "Common/safe_cast.h"

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

WriteBufferFromCeph::WriteBufferFromCeph(
    std::shared_ptr<librados::Rados> rados_,
    const String & pool,
    const String & object_id_,
    const WriteSettings & write_settings_,
    std::optional<std::map<String, String>> object_attributes_,
    size_t buf_size_,
    WriteMode mode_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , object_id(object_id_)
    , write_settings(write_settings_)
    , object_attributes(std::move(object_attributes_))
    , mode(mode_)
{
    impl = std::make_unique<Ceph::RadosIO>(std::move(rados_), pool, "", false);
}


WriteBufferFromCeph::WriteBufferFromCeph(
    std::unique_ptr<Ceph::RadosIO> impl_,
    const String & object_id_,
    const WriteSettings & write_settings_,
    std::optional<std::map<String, String>> object_attributes_,
    size_t buf_size_,
    WriteMode mode_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , impl(std::move(impl_))
    , object_id(object_id_)
    , write_settings(write_settings_)
    , object_attributes(std::move(object_attributes_))
    , mode(mode_) {}

void WriteBufferFromCeph::initialize()
{
    if (initialized)
        return;

    if (!impl)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadosClient is not initialized");

    impl->connect();

    initialized = true;
}

void WriteBufferFromCeph::finalizeImpl()
{
    if (!initialized)
        return;
    WriteBufferFromFileBase::finalizeImpl();
    setObjectAttributes();
}

void WriteBufferFromCeph::setObjectAttributes()
{
    if (object_attributes)
        impl->setAttributes(object_id, *object_attributes);
}

size_t WriteBufferFromCeph::writeImpl(const char * begin, size_t len)
{
    ResourceGuard rlock(write_settings.resource_link, len);
    size_t bytes_written = 0;
    try
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::WriteBufferFromCephMicroseconds);
        if (mode == WriteMode::Rewrite && first_write)
        {
            /// O_CREATE | O_TRUNC
            bytes_written = impl->writeFull(object_id, begin, len);
            first_write = false;
        }
        else
            bytes_written = impl->append(object_id, begin, len);

        if (write_settings.remote_throttler)
            write_settings.remote_throttler->add(len, ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);
    }
    catch (...)
    {
        write_settings.resource_link.accumulate(len); // We assume no resource was used in case of failure
        throw;
    }
    rlock.unlock();

    write_settings.resource_link.adjust(len, bytes_written);
    ProfileEvents::increment(ProfileEvents::WriteBufferFromCephBytes, bytes_written);
    return bytes_written;
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
