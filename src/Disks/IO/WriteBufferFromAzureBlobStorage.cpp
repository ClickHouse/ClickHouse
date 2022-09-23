#include <Common/config.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/Throttler.h>


namespace DB
{

WriteBufferFromAzureBlobStorage::WriteBufferFromAzureBlobStorage(
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
    const String & blob_path_,
    size_t max_single_part_upload_size_,
    size_t buf_size_,
    const WriteSettings & write_settings_)
    : BufferWithOwnMemory<WriteBuffer>(buf_size_, nullptr, 0)
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

void WriteBufferFromAzureBlobStorage::execWithRetry(std::function<void()> func, size_t num_tries)
{
    auto can_retry_exception = [&]<typename Exception>(const Exception & e, size_t i) -> bool
    {
        if (i == num_tries - 1)
            return false;

        LOG_DEBUG(log, "Write at attempt {} for blob `{}` failed: {}", i + 1, blob_path, e.Message);
        return true;
    };

    for (size_t i = 0; i < num_tries; ++i)
    {
        try
        {
            func();
            break;
        }
        catch (const Azure::Core::Http::TransportException & e)
        {
            if (!can_retry_exception(e, i))
                throw;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            if (!can_retry_exception(e, i))
                throw;
        }
    }
}

void WriteBufferFromAzureBlobStorage::finalizeImpl()
{
    execWithRetry([this](){ next(); }, 3);
}

void WriteBufferFromAzureBlobStorage::nextImpl()
{
    if (!offset())
        return;

    char * buffer_begin = working_buffer.begin();
    size_t total_size = offset();

    auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);

    size_t current_size = 0;
    std::vector<std::string> block_ids;

    while (current_size < total_size)
    {
        size_t part_len = std::min(total_size - current_size, max_single_part_upload_size);
        const std::string & block_id = block_ids.emplace_back(getRandomASCIIString(64));

        Azure::Core::IO::MemoryBodyStream tmp_buffer(reinterpret_cast<uint8_t *>(buffer_begin + current_size), part_len);
        execWithRetry([&block_blob_client, &block_id, &tmp_buffer](){ block_blob_client.StageBlock(block_id, tmp_buffer); }, 3);

        current_size += part_len;
        LOG_TRACE(log, "Staged block (id: {}) of size {} (written {}/{}, blob path: {}).", block_id, part_len, current_size, total_size, blob_path);
    }

    execWithRetry([&block_blob_client, &block_ids](){ block_blob_client.CommitBlockList(block_ids); }, 3);
    LOG_TRACE(log, "Commited {} blocks for blob `{}`", block_ids.size(), blob_path);

    if (write_settings.remote_throttler)
        write_settings.remote_throttler->add(total_size);
}

}

#endif
