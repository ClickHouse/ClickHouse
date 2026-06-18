#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/Throttler.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int LOGICAL_ERROR;
}

/// `PartData` is `AzureBlobStorage::UploadPartData` (see the using-alias in
/// WriteBufferFromAzureBlobStorage.h). The struct is shared with the wrapper
/// so callers can hand the payload over by reference without copying or
/// re-casting.

static BufferAllocationPolicyPtr createBufferAllocationPolicy(const AzureBlobStorage::RequestSettings & settings)
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
    AzureClientPtr blob_container_client_,
    const String & blob_path_,
    size_t buf_size_,
    const WriteSettings & write_settings_,
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings_,
    const String & container_for_logging_,
    BlobStorageLogWriterPtr blob_log_,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_)
    : WriteBufferFromFileBase(std::min(buf_size_, static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE)), nullptr, 0)
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
              limited_log))
    , check_objects_after_upload(settings_->check_objects_after_upload)
    , container_for_logging(container_for_logging_)
    , blob_log(std::move(blob_log_))
{
    allocateBuffer();
}


WriteBufferFromAzureBlobStorage::~WriteBufferFromAzureBlobStorage()
{
    LOG_TRACE(limited_log, "Close WriteBufferFromAzureBlobStorage. {}.", blob_path);

    if (canceled)
    {
        if (!isEmpty())
        {
            LOG_INFO(
                log,
                "WriteBufferFromAzureBlobStorage was canceled."
                "The file might not be written to AzureBlobStorage. "
                "{}.",
                blob_path);
        }
    }
    else if (!finalized)
    {
        /// That destructor could be call with finalized=false in case of exceptions
        LOG_INFO(
            log,
            "WriteBufferFromAzureBlobStorage is not finalized in destructor. "
            "The file might not be written to AzureBlobStorage. "
            "{}.",
            blob_path);
    }

    task_tracker->safeWaitAll();
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

    if (block_ids.empty())
    {
        /// If there is only one block and size is less than or equal to max_single_part_upload_size
        /// then we use single part upload instead of multi part upload
        if (detached_part_data.size() == 1 && detached_part_data.front().data_size <= max_single_part_upload_size)
        {
            auto part_data = std::move(detached_part_data.front());
            blob_container_client->uploadSinglePart(
                blob_path, container_for_logging,
                reinterpret_cast<const uint8_t *>(part_data.memory.data()),
                part_data.data_size,
                &write_settings,
                max_unexpected_write_error_retries, log,
                blob_log);
            LOG_TRACE(limited_log, "Committed single block for blob `{}`", blob_path);
            detached_part_data.pop_front();
            return;
        }
        /// Upload a single empty block
        else if (detached_part_data.empty())
        {
            blob_container_client->uploadSinglePart(
                blob_path, container_for_logging,
                nullptr,
                0,
                &write_settings,
                max_unexpected_write_error_retries, log,
                blob_log);
            LOG_TRACE(log, "Committed single empty block for blob `{}`", blob_path);
            return;
        }
    }

    writeMultipartUpload();
}

void WriteBufferFromAzureBlobStorage::finalizeImpl()
{
    LOG_TRACE(limited_log, "finalizeImpl WriteBufferFromAzureBlobStorage {}", blob_path);

    if (!is_prefinalized)
        preFinalize();

    chassert(offset() == 0);
    chassert(hidden_size == 0);

    task_tracker->waitAll();

    if (!block_ids.empty())
    {
        blob_container_client->commitBlockList(
            blob_path, container_for_logging, block_ids, &write_settings,
            max_unexpected_write_error_retries, log,
            blob_log);
        LOG_TRACE(limited_log, "Committed {} blocks for blob `{}`", block_ids.size(), blob_path);
    }

    if (check_objects_after_upload)
    {
        try
        {
            blob_container_client->GetBlobProperties(blob_path);
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
                throw Exception(
                        ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                        "Object {} not uploaded to azure blob storage, it's a bug in Azure Blob Storage or its API.",
                        blob_path);
            throw;
        }
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
        write_settings.remote_throttler->throttle(offset());

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

    /// First buffer was already allocated in BufferWithOwnMemory constructor with buffer size provided in constructor.
    /// It will be reallocated in subsequent nextImpl calls up to the desired buffer size from buffer_allocation_policy.
    if (buffer_allocation_policy->getBufferNumber() == 1)
    {
        /// Reduce memory size if initial size was larger then desired size from buffer_allocation_policy.
        /// Usually it doesn't happen but we have it in unit tests.
        if (memory.size() > buffer_allocation_policy->getBufferSize())
        {
            memory.resize(buffer_allocation_policy->getBufferSize());
            WriteBuffer::set(memory.data(), memory.size());
        }
        return;
    }

    auto size = buffer_allocation_policy->getBufferSize();
    memory = Memory<>(size);
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
        const auto & data_block_id = std::get<0>(*worker_data);
        const auto & worker_part_data = std::get<1>(*worker_data);

        blob_container_client->stageBlock(
            blob_path, container_for_logging, data_block_id,
            reinterpret_cast<const uint8_t *>(worker_part_data.memory.data()),
            worker_part_data.data_size,
            &write_settings,
            max_unexpected_write_error_retries, log,
            blob_log);
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
