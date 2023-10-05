#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/Throttler.h>
#include <IO/ResourceGuard.h>


namespace ProfileEvents
{
    extern const Event RemoteWriteThrottlerBytes;
    extern const Event RemoteWriteThrottlerSleepMicroseconds;
}

namespace DB
{

static constexpr auto DEFAULT_RETRY_NUM = 3;

WriteBufferFromAzureBlobStorage::WriteBufferFromAzureBlobStorage(
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
    const String & blob_path_,
    size_t max_single_part_upload_size_,
    size_t buf_size_,
    const WriteSettings & write_settings_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , log(&Poco::Logger::get("WriteBufferFromAzureBlobStorage"))
    , max_single_part_upload_size(max_single_part_upload_size_)
    , blob_path(blob_path_)
    , write_settings(write_settings_)
    , blob_container_client(blob_container_client_)
{
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
        catch (const Azure::Core::Http::TransportException & e)
        {
            handle_exception(e, i);
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
    execWithRetry([this](){ next(); }, DEFAULT_RETRY_NUM);

    if (tmp_buffer_write_offset > 0)
        uploadBlock(tmp_buffer->data(), tmp_buffer_write_offset);

    auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);
    execWithRetry([&](){ block_blob_client.CommitBlockList(block_ids); }, DEFAULT_RETRY_NUM);

    LOG_TRACE(log, "Committed {} blocks for blob `{}`", block_ids.size(), blob_path);
}

void WriteBufferFromAzureBlobStorage::uploadBlock(const char * data, size_t size)
{
    auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);
    const std::string & block_id = block_ids.emplace_back(getRandomASCIIString(64));

    Azure::Core::IO::MemoryBodyStream memory_stream(reinterpret_cast<const uint8_t *>(data), size);
    execWithRetry([&](){ block_blob_client.StageBlock(block_id, memory_stream); }, DEFAULT_RETRY_NUM, size);
    tmp_buffer_write_offset = 0;

    LOG_TRACE(log, "Staged block (id: {}) of size {} (blob path: {}).", block_id, size, blob_path);
}

WriteBufferFromAzureBlobStorage::MemoryBufferPtr WriteBufferFromAzureBlobStorage::allocateBuffer() const
{
    return std::make_unique<Memory<>>(max_single_part_upload_size);
}

void WriteBufferFromAzureBlobStorage::nextImpl()
{
    size_t size_to_upload = offset();

    if (size_to_upload == 0)
        return;

    if (!tmp_buffer)
        tmp_buffer = allocateBuffer();

    size_t uploaded_size = 0;
    while (uploaded_size != size_to_upload)
    {
        size_t memory_buffer_remaining_size = max_single_part_upload_size - tmp_buffer_write_offset;
        if (memory_buffer_remaining_size == 0)
            uploadBlock(tmp_buffer->data(), tmp_buffer->size());

        size_t size = std::min(memory_buffer_remaining_size, size_to_upload - uploaded_size);
        memcpy(tmp_buffer->data() + tmp_buffer_write_offset, working_buffer.begin() + uploaded_size, size);
        uploaded_size += size;
        tmp_buffer_write_offset += size;
    }

    if (tmp_buffer_write_offset == max_single_part_upload_size)
        uploadBlock(tmp_buffer->data(), tmp_buffer->size());

    if (write_settings.remote_throttler)
        write_settings.remote_throttler->add(size_to_upload, ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);
}

}

#endif
