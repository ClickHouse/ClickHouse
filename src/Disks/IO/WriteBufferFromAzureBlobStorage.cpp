#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/Throttler.h>
#include <Common/Scheduler/ResourceGuard.h>


namespace ProfileEvents
{
    extern const Event RemoteWriteThrottlerBytes;
    extern const Event RemoteWriteThrottlerSleepMicroseconds;
}

namespace DB
{

struct WriteBufferFromAzureBlobStorage::PartData
{
    Memory<> memory;
    size_t data_size = 0;
};

WriteBufferFromAzureBlobStorage::WriteBufferFromAzureBlobStorage(
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
    const String & blob_path_,
    size_t buf_size_,
    const WriteSettings & write_settings_,
    std::shared_ptr<const AzureObjectStorageSettings> settings_,
    ThreadPoolCallbackRunner<void> schedule_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , log(getLogger("WriteBufferFromAzureBlobStorage"))
    , buffer_allocation_policy(IBufferAllocationPolicy::create({settings_->strict_upload_part_size,
                                                                settings_->min_upload_part_size,
                                                                settings_->max_upload_part_size,
                                                                settings_->upload_part_size_multiply_factor,
                                                                settings_->upload_part_size_multiply_parts_count_threshold,
                                                                settings_->max_single_part_upload_size}))
    , max_single_part_upload_size(settings_->max_single_part_upload_size)
    , max_unexpected_write_error_retries(settings_->max_unexpected_write_error_retries)
    , blob_path(blob_path_)
    , write_settings(write_settings_)
    , blob_container_client(blob_container_client_)
    , task_tracker(
          std::make_unique<TaskTracker>(
              std::move(schedule_),
              settings_->max_inflight_parts_for_one_file,
              limitedLog))
{
    allocateBuffer();
}


WriteBufferFromAzureBlobStorage::~WriteBufferFromAzureBlobStorage()
{
    finalize();
}

void WriteBufferFromAzureBlobStorage::execWithRetry(std::function<void()> func, size_t num_tries, size_t cost)
{
    auto handle_exception = [&, this](const auto & e, size_t i)
    {
        if (cost)
            write_settings.resource_link.accumulate(cost); // Accumulate resource for later use, because we have failed to consume it

        if (i == num_tries - 1)
            throw;

        LOG_DEBUG(log, "Write at attempt {} for blob `{}` failed: {} {}", i + 1, blob_path, e.what(), e.Message);
    };

    for (size_t i = 0; i < num_tries; ++i)
    {
        try
        {
            ResourceGuard rlock(write_settings.resource_link, cost); // Note that zero-cost requests are ignored
            func();
            break;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            handle_exception(e, i);
        }
        catch (...)
        {
            if (cost)
                write_settings.resource_link.accumulate(cost); // We assume no resource was used in case of failure
            throw;
        }
    }
}

void WriteBufferFromAzureBlobStorage::finalizeImpl()
{
    execWithRetry([this](){ next(); }, max_unexpected_write_error_retries);

    task_tracker->waitAll();

    auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);
    execWithRetry([&](){ block_blob_client.CommitBlockList(block_ids); }, max_unexpected_write_error_retries);

    LOG_TRACE(log, "Committed {} blocks for blob `{}`", block_ids.size(), blob_path);
}

void WriteBufferFromAzureBlobStorage::nextImpl()
{
    task_tracker->waitIfAny();

    reallocateBuffer();
    detachBuffer();

    while (!detached_part_data.empty())
    {
        writePart(std::move(detached_part_data.front()));
        detached_part_data.pop_front();
    }

    allocateBuffer();
}

void WriteBufferFromAzureBlobStorage::allocateBuffer()
{
    buffer_allocation_policy->nextBuffer();
    memory = Memory(buffer_allocation_policy->getBufferSize());
    WriteBuffer::set(memory.data(), memory.size());
}


void WriteBufferFromAzureBlobStorage::reallocateBuffer()
{
    if (available() > 0)
        return;

    if (memory.size() == buffer_allocation_policy->getBufferSize())
        return;

    memory.resize(buffer_allocation_policy->getBufferSize());

    WriteBuffer::set(memory.data(), memory.size());

    chassert(offset() == 0);
}

void WriteBufferFromAzureBlobStorage::detachBuffer()
{
    size_t data_size = size_t(position() - memory.data());
    auto buf = std::move(memory);
    WriteBuffer::set(nullptr, 0);
    detached_part_data.push_back({std::move(buf), data_size});
}

void WriteBufferFromAzureBlobStorage::writePart(WriteBufferFromAzureBlobStorage::PartData && data)
{
    if (data.data_size == 0)
        return;

    auto upload_worker = [&] ()
    {
        auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);
        const std::string & block_id = block_ids.emplace_back(getRandomASCIIString(64));

        Azure::Core::IO::MemoryBodyStream memory_stream(reinterpret_cast<const uint8_t *>(data.memory.data()), data.data_size);
        execWithRetry([&](){ block_blob_client.StageBlock(block_id, memory_stream); }, max_unexpected_write_error_retries, data.data_size);

        if (write_settings.remote_throttler)
            write_settings.remote_throttler->add(data.data_size, ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);
    };

    task_tracker->add(std::move(upload_worker));
}

}

#endif
