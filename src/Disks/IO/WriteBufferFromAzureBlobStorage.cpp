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

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct WriteBufferFromAzureBlobStorage::PartData
{
    Memory<> memory;
    size_t data_size = 0;
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

    hidePartialData();

    if (hidden_size > 0)
        detachBuffer();

    setFakeBufferWhenPreFinalized();

    /// If there is only one block and size is less than or equal to max_single_part_upload_size
    /// then we use single part upload instead of multi part upload
    if (block_ids.empty() && detached_part_data.size() == 1 && detached_part_data.front().data_size <= max_single_part_upload_size)
    {
        auto part_data = std::move(detached_part_data.front());
        auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);
        Azure::Core::IO::MemoryBodyStream memory_stream(reinterpret_cast<const uint8_t *>(part_data.memory.data()), part_data.data_size);
        execWithRetry([&](){ block_blob_client.Upload(memory_stream); }, max_unexpected_write_error_retries, part_data.data_size);
        LOG_TRACE(log, "Committed single block for blob `{}`", blob_path);

        detached_part_data.pop_front();
        return;
    }
    else
    {
        writeMultipartUpload();
    }
}

void WriteBufferFromAzureBlobStorage::finalizeImpl()
{
    LOG_TRACE(log, "finalizeImpl WriteBufferFromAzureBlobStorage {}", blob_path);

    if (!is_prefinalized)
        preFinalize();

    chassert(offset() == 0);
    chassert(hidden_size == 0);

    task_tracker->waitAll();

    if (!block_ids.empty())
    {
        auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);
        execWithRetry([&](){ block_blob_client.CommitBlockList(block_ids); }, max_unexpected_write_error_retries);
        LOG_TRACE(log, "Committed {} blocks for blob `{}`", block_ids.size(), blob_path);
    }
}

void WriteBufferFromAzureBlobStorage::nextImpl()
{
    if (is_prefinalized)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot write to prefinalized buffer for Azure Blob Storage, the file could have been created");

    task_tracker->waitIfAny();

    hidePartialData();

    reallocateFirstBuffer();

    if (available() > 0)
        return;

    detachBuffer();

    if (detached_part_data.size() > 1)
        writeMultipartUpload();

    allocateBuffer();
}

void WriteBufferFromAzureBlobStorage::hidePartialData()
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

void WriteBufferFromAzureBlobStorage::reallocateFirstBuffer()
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

void WriteBufferFromAzureBlobStorage::allocateBuffer()
{
    buffer_allocation_policy->nextBuffer();
    chassert(0 == hidden_size);

    auto size = buffer_allocation_policy->getBufferSize();

    if (buffer_allocation_policy->getBufferNumber() == 1)
        size = std::min(size_t(DBMS_DEFAULT_BUFFER_SIZE), size);

    memory = Memory(size);
    WriteBuffer::set(memory.data(), memory.size());
}

void WriteBufferFromAzureBlobStorage::detachBuffer()
{
    size_t data_size = size_t(position() - memory.data());
    if (data_size == 0)
        return;

    chassert(data_size == hidden_size);

    auto buf = std::move(memory);

    WriteBuffer::set(nullptr, 0);
    total_size += hidden_size;
    hidden_size = 0;

    detached_part_data.push_back({std::move(buf), data_size});
    WriteBuffer::set(nullptr, 0);
}

void WriteBufferFromAzureBlobStorage::writePart(WriteBufferFromAzureBlobStorage::PartData && part_data)
{
    const std::string & block_id = block_ids.emplace_back(getRandomASCIIString(64));
    auto worker_data = std::make_shared<std::tuple<std::string, WriteBufferFromAzureBlobStorage::PartData>>(block_id, std::move(part_data));

    auto upload_worker = [this, worker_data] ()
    {
        auto & data_size = std::get<1>(*worker_data).data_size;
        auto & data_block_id = std::get<0>(*worker_data);
        auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);

        Azure::Core::IO::MemoryBodyStream memory_stream(reinterpret_cast<const uint8_t *>(std::get<1>(*worker_data).memory.data()), data_size);
        execWithRetry([&](){ block_blob_client.StageBlock(data_block_id, memory_stream); }, max_unexpected_write_error_retries, data_size);
    };

    task_tracker->add(std::move(upload_worker));
}

void WriteBufferFromAzureBlobStorage::setFakeBufferWhenPreFinalized()
{
    WriteBuffer::set(fake_buffer_when_prefinalized, sizeof(fake_buffer_when_prefinalized));
}

void WriteBufferFromAzureBlobStorage::writeMultipartUpload()
{
    while (!detached_part_data.empty())
    {
        writePart(std::move(detached_part_data.front()));
        detached_part_data.pop_front();
    }
}

}

#endif
