#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
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
    std::string block_id;
};

BufferAllocationPolicyPtr createBufferAllocationPolicy(const AzureObjectStorageSettings & settings)
{
    BufferAllocationPolicy::Settings allocation_settings;
    allocation_settings.strict_size = settings.strict_upload_part_size;
    allocation_settings.min_size = settings.min_upload_part_size;
    allocation_settings.max_size = settings.max_upload_part_size;
    allocation_settings.multiply_factor = settings.upload_part_size_multiply_factor;
    allocation_settings.multiply_parts_count_threshold = settings.upload_part_size_multiply_parts_count_threshold;
    allocation_settings.max_single_size = settings.max_single_part_upload_size;

    return BufferAllocationPolicy::create(allocation_settings);
}

WriteBufferFromAzureBlobStorage::WriteBufferFromAzureBlobStorage(
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
    const String & blob_path_,
    size_t buf_size_,
    const WriteSettings & write_settings_,
    std::shared_ptr<const AzureObjectStorageSettings> settings_,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , log(getLogger("WriteBufferFromAzureBlobStorage"))
    , buffer_allocation_policy(createBufferAllocationPolicy(*settings_))
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
    LOG_TRACE(limitedLog, "Close WriteBufferFromAzureBlobStorage. {}.", blob_path);

    /// That destructor could be call with finalized=false in case of exceptions
    if (!finalized)
    {
        LOG_INFO(
            log,
            "WriteBufferFromAzureBlobStorage is not finalized in destructor. "
            "The file might not be written to AzureBlobStorage. "
            "{}.",
            blob_path);
    }

    task_tracker->safeWaitAll();
}

void WriteBufferFromAzureBlobStorage::execWithRetry(std::function<void()> func, size_t num_tries, size_t cost)
{
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
            if (cost)
                write_settings.resource_link.accumulate(cost); // Accumulate resource for later use, because we have failed to consume it

            if (i == num_tries - 1 || !isRetryableAzureException(e))
                throw;

            LOG_DEBUG(log, "Write at attempt {} for blob `{}` failed: {} {}", i + 1, blob_path, e.what(), e.Message);
        }
        catch (...)
        {
            if (cost)
                write_settings.resource_link.accumulate(cost); // We assume no resource was used in case of failure
            throw;
        }
    }
}

void WriteBufferFromAzureBlobStorage::preFinalize()
{
    if (is_prefinalized)
        return;

    // This function should not be run again
    is_prefinalized = true;

    /// If there is only one block and size is less than or equal to max_single_part_upload_size
    /// then we use single part upload instead of multi part upload
    if (buffer_allocation_policy->getBufferNumber() == 1)
    {
        size_t data_size = size_t(position() - memory.data());
        if (data_size <= max_single_part_upload_size)
        {
            auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);
            Azure::Core::IO::MemoryBodyStream memory_stream(reinterpret_cast<const uint8_t *>(memory.data()), data_size);
            execWithRetry([&](){ block_blob_client.Upload(memory_stream); }, max_unexpected_write_error_retries, data_size);
            LOG_TRACE(log, "Committed single block for blob `{}`", blob_path);
            return;
        }
    }

    writePart();
}

void WriteBufferFromAzureBlobStorage::finalizeImpl()
{
    LOG_TRACE(log, "finalizeImpl WriteBufferFromAzureBlobStorage {}", blob_path);

    if (!is_prefinalized)
        preFinalize();

    if (!block_ids.empty())
    {
        task_tracker->waitAll();
        auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);
        execWithRetry([&](){ block_blob_client.CommitBlockList(block_ids); }, max_unexpected_write_error_retries);
        LOG_TRACE(log, "Committed {} blocks for blob `{}`", block_ids.size(), blob_path);
    }
}

void WriteBufferFromAzureBlobStorage::nextImpl()
{
    task_tracker->waitIfAny();
    writePart();
    allocateBuffer();
}

void WriteBufferFromAzureBlobStorage::allocateBuffer()
{
    buffer_allocation_policy->nextBuffer();
    auto size = buffer_allocation_policy->getBufferSize();

    if (buffer_allocation_policy->getBufferNumber() == 1)
        size = std::min(size_t(DBMS_DEFAULT_BUFFER_SIZE), size);

    memory = Memory(size);
    WriteBuffer::set(memory.data(), memory.size());
}

void WriteBufferFromAzureBlobStorage::writePart()
{
    auto data_size = size_t(position() - memory.data());
    if (data_size == 0)
        return;

    const std::string & block_id = block_ids.emplace_back(getRandomASCIIString(64));
    std::shared_ptr<PartData> part_data = std::make_shared<PartData>(std::move(memory), data_size, block_id);
    WriteBuffer::set(nullptr, 0);

    auto upload_worker = [this, part_data] ()
    {
        auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);

        Azure::Core::IO::MemoryBodyStream memory_stream(reinterpret_cast<const uint8_t *>(part_data->memory.data()), part_data->data_size);
        execWithRetry([&](){ block_blob_client.StageBlock(part_data->block_id, memory_stream); }, max_unexpected_write_error_retries, part_data->data_size);

        if (write_settings.remote_throttler)
            write_settings.remote_throttler->add(part_data->data_size, ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);
    };

    task_tracker->add(std::move(upload_worker));
}

}

#endif
