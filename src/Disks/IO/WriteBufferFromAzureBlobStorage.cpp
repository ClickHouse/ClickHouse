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
    const WriteSettings & write_settings_,
    std::optional<std::map<std::string, std::string>> attributes_)
    : BufferWithOwnMemory<WriteBuffer>(buf_size_, nullptr, 0)
    , blob_container_client(blob_container_client_)
    , max_single_part_upload_size(max_single_part_upload_size_)
    , blob_path(blob_path_)
    , write_settings(write_settings_)
    , attributes(attributes_)
{
}

void WriteBufferFromAzureBlobStorage::finalizeImpl()
{
    if (attributes.has_value())
    {
        auto blob_client = blob_container_client->GetBlobClient(blob_path);
        Azure::Storage::Metadata metadata;
        for (const auto & [key, value] : *attributes)
            metadata[key] = value;
        blob_client.SetMetadata(metadata);
    }

    const size_t max_tries = 3;
    for (size_t i = 0; i < max_tries; ++i)
    {
        try
        {
            next();
            break;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            if (i == max_tries - 1)
                throw;
            LOG_INFO(&Poco::Logger::get("WriteBufferFromAzureBlobStorage"),
                     "Exception caught during finalizing azure storage write at attempt {}: {}", i + 1, e.Message);
        }
    }
}

void WriteBufferFromAzureBlobStorage::nextImpl()
{
    if (!offset())
        return;

    auto * buffer_begin = working_buffer.begin();
    auto len = offset();
    auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);

    size_t read = 0;
    std::vector<std::string> block_ids;
    while (read < len)
    {
        auto part_len = std::min(len - read, max_single_part_upload_size);

        auto block_id = getRandomASCIIString(64);
        block_ids.push_back(block_id);

        Azure::Core::IO::MemoryBodyStream tmp_buffer(reinterpret_cast<uint8_t *>(buffer_begin + read), part_len);
        block_blob_client.StageBlock(block_id, tmp_buffer);

        read += part_len;
    }

    block_blob_client.CommitBlockList(block_ids);

    if (write_settings.remote_throttler)
        write_settings.remote_throttler->add(read);
}

}

#endif
