#include "Disks/ObjectStorages/Ceph/CephObjectStorage.h"
#include "config.h"

#if USE_AWS_S3

#include "StriperWriteBufferFromRados.h"

#include <Common/ThreadPoolTaskTracker.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/Throttler.h>
#include <Interpreters/Cache/FileCache.h>

#include <Common/Scheduler/ResourceGuard.h>
#include <IO/WriteHelpers.h>
#include <IO/S3Common.h>
#include <IO/S3/Requests.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/S3/BlobStorageLogWriter.h>

#include <utility>


namespace ProfileEvents
{
    extern const Event WriteBufferFromCephBytes;
    extern const Event WriteBufferFromCephMicroseconds;
    extern const Event RemoteWriteThrottlerBytes;
    extern const Event RemoteWriteThrottlerSleepMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CEPH_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
}

struct StriperWriteBufferFromRados::PartData
{
    Memory<> memory;
    size_t data_size = 0;

    bool isEmpty() const
    {
        return data_size == 0;
    }
};

BufferAllocationPolicyPtr createBufferAllocationPolicy(size_t max_object_size)
{
    BufferAllocationPolicy::Settings allocation_settings;
    allocation_settings.strict_size = max_object_size;
    return BufferAllocationPolicy::create(allocation_settings);
}


StriperWriteBufferFromRados::StriperWriteBufferFromRados(
    std::shared_ptr<Ceph::RadosIO> impl_,
    const String & object_id_,
    size_t buf_size_,
    size_t max_object_size_,
    const WriteSettings & write_settings_,
    std::optional<std::map<String, String>> object_metadata_,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , impl(std::move(impl_))
    , object_id(object_id_)
    , max_object_size(max_object_size_)
    , write_settings(write_settings_)
    , object_metadata(std::move(object_metadata_))
    , buffer_allocation_policy(createBufferAllocationPolicy(max_object_size))
    , task_tracker(
          std::make_unique<TaskTracker>(
              std::move(schedule_),
              1,
              limitedLog))
{
    LOG_TRACE(limitedLog, "Create StriperWriteBufferFromRados, {}", getShortLogDetails());

    allocateBuffer();
}

void StriperWriteBufferFromRados::nextImpl()
{
    if (is_prefinalized)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot write to prefinalized buffer for Rados");

    /// Make sense to call waitIfAny before adding new async task to check if there is an exception
    /// The faster the exception is propagated the lesser time is spent for cancellation
    /// Despite the fact that `task_tracker->add()` collects tasks statuses and propagates their exceptions
    /// that call is necessary for the case when the is no in-flight limitation and therefore `task_tracker->add()` doesn't wait anything
    task_tracker->waitIfAny();

    hidePartialData();

    reallocateFirstBuffer();

    if (available() > 0)
        return;

    detachBuffer();

    if (detached_part_data.size() > 1)
    {
        writeMultipartUpload();
    }

    allocateBuffer();
}

void StriperWriteBufferFromRados::preFinalize()
{
    if (is_prefinalized)
        return;

    LOG_TEST(log, "preFinalize StriperWriteBufferFromRados. {}", getShortLogDetails());

    /// This function should not be run again if an exception has occurred
    is_prefinalized = true;

    hidePartialData();

    if (hidden_size > 0)
        detachBuffer();
    setFakeBufferWhenPreFinalized();
    writeMultipartUpload();
}

void StriperWriteBufferFromRados::finalizeImpl()
{
    LOG_TRACE(log, "finalizeImpl StriperWriteBufferFromRados. {}.", getShortLogDetails());

    if (!is_prefinalized)
        preFinalize();

    chassert(offset() == 0);
    chassert(hidden_size == 0);

    task_tracker->waitAll();

    /// If object has multiple parts, we need to update the metadata in HEAD object
    if (object_seq > 1)
    {
        std::map<String, String> metadata;
        metadata[RadosStriper::XATTR_OBJECT_COUNT] = std::to_string(object_seq);
        metadata[RadosStriper::XATTR_OBJECT_SIZE] = std::to_string(total_size);
        metadata[RadosStriper::XATTR_OBJECT_MTIME] = std::to_string(time(nullptr));
        if (object_metadata)
            metadata.insert(object_metadata->begin(), object_metadata->end());
        impl->setAttributes(object_id, metadata);
    }
}

String StriperWriteBufferFromRados::getVerboseLogDetails() const
{
    return fmt::format("Details: object {}, total size {}, count {}, hidden_size {}, offset {}, with pool: {}, prefinalized {}, finalized {}",
                       object_id, total_size, count(), hidden_size, offset(), task_tracker->isAsync(), is_prefinalized, finalized);
}

String StriperWriteBufferFromRados::getShortLogDetails() const
{
    return fmt::format("Details: Object {}", object_id);
}

StriperWriteBufferFromRados::~StriperWriteBufferFromRados()
{
    LOG_TRACE(limitedLog, "Close StriperWriteBufferFromRados. {}.", getShortLogDetails());

    if (canceled)
    {
        LOG_INFO(
            log,
            "StriperWriteBufferFromRados was canceled."
            "The file might not be written to Rados. "
            "{}.",
            getVerboseLogDetails());
        return;
    }

    /// That destructor could be call with finalized=false in case of exceptions
    if (!finalized && !canceled)
    {
        LOG_INFO(
            log,
            "StriperWriteBufferFromRados is not finalized in destructor. "
            "The file might not be written to Rados. "
            "{}.",
            getVerboseLogDetails());
    }

    task_tracker->safeWaitAll();
}

void StriperWriteBufferFromRados::hidePartialData()
{
    if (write_settings.remote_throttler)
            write_settings.remote_throttler->add(offset(), ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);

    chassert(memory.size() >= hidden_size + offset());

    hidden_size += offset();
    chassert(memory.data() + hidden_size == working_buffer.begin() + offset());
    chassert(memory.data() + hidden_size == position());

    WriteBuffer::set(memory.data() + hidden_size, memory.size() - hidden_size);
    chassert(offset() == 0);
}

void StriperWriteBufferFromRados::reallocateFirstBuffer()
{
    chassert(offset() == 0);

    if (buffer_allocation_policy->getBufferNumber() > 1 || available() > 0)
        return;

    const size_t max_first_buffer = buffer_allocation_policy->getBufferSize();
    if (memory.size() == max_first_buffer)
        return;

    size_t size = std::min(memory.size() * 2, max_first_buffer);
    memory.resize(size);

    WriteBuffer::set(memory.data() + hidden_size, memory.size() - hidden_size);

    chassert(offset() == 0);
}

void StriperWriteBufferFromRados::detachBuffer()
{
    size_t data_size = size_t(position() - memory.data());
    chassert(data_size == hidden_size);

    auto buf = std::move(memory);

    WriteBuffer::set(nullptr, 0);
    total_size += hidden_size;
    hidden_size = 0;

    detached_part_data.push_back({std::move(buf), data_size});
}

void StriperWriteBufferFromRados::allocateBuffer()
{
    buffer_allocation_policy->nextBuffer();
    chassert(0 == hidden_size);

    if (buffer_allocation_policy->getBufferNumber() == 1)
    {
        allocateFirstBuffer();
        return;
    }

    memory = Memory(buffer_allocation_policy->getBufferSize());
    WriteBuffer::set(memory.data(), memory.size());
}

void StriperWriteBufferFromRados::allocateFirstBuffer()
{
    const auto max_first_buffer = buffer_allocation_policy->getBufferSize();
    const auto size = std::min(size_t(DBMS_DEFAULT_BUFFER_SIZE), max_first_buffer);
    memory = Memory(size);
    WriteBuffer::set(memory.data(), memory.size());
}

void StriperWriteBufferFromRados::setFakeBufferWhenPreFinalized()
{
    WriteBuffer::set(fake_buffer_when_prefinalized, sizeof(fake_buffer_when_prefinalized));
}

void StriperWriteBufferFromRados::writeMultipartUpload()
{
    while (!detached_part_data.empty())
    {
        writePart(std::move(detached_part_data.front()));
        detached_part_data.pop_front();
    }
}

void StriperWriteBufferFromRados::writePart(StriperWriteBufferFromRados::PartData && data)
{
    if (data.data_size == 0)
    {
        LOG_TEST(log, "Skipping writing part as empty {}", getShortLogDetails());
        return;
    }

    LOG_TEST(log, "writePart {}, part size {}, part number {}", getShortLogDetails(), data.data_size, object_seq);

    if (data.data_size > max_object_size)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Part size exceeded max_upload_part_size. {}, part number {}, part size {}, max_upload_part_size {}",
            getShortLogDetails(),
            object_seq,
            data.data_size,
            max_object_size
            );
    }

    String rados_object_name = object_seq == 0 ? object_id : RadosStriper::getStripedObjectName(object_id, object_seq);
    if (object_seq == 0)
        /// This is the head object, we create it first because we need it exists to store metadata
        impl->create(rados_object_name, true);
    else
        /// update the stripe count in HEAD object
        impl->setAttribute(object_id, RadosStriper::XATTR_OBJECT_COUNT, std::to_string(object_seq+1));
    object_seq++;

    auto upload_worker = [this, content = std::make_shared<PartData>(std::move(data)), object_name = std::move(rados_object_name)] ()
    {
        ResourceGuard rlock(write_settings.resource_link, content->data_size);
        size_t len = content->data_size;
        try
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::WriteBufferFromCephMicroseconds);
            /// O_CREATE | O_TRUNC
            impl->writeFull(object_name, content->memory.data(), len);

            if (write_settings.remote_throttler)
                write_settings.remote_throttler->add(len, ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);
        }
        catch (...)
        {
            write_settings.resource_link.accumulate(len); // We assume no resource was used in case of failure
            throw;
        }
        rlock.unlock();

        write_settings.resource_link.adjust(len, len);
        ProfileEvents::increment(ProfileEvents::WriteBufferFromCephBytes, len);
    };

    task_tracker->add(std::move(upload_worker));
}

}

#endif
