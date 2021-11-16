#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <IO/WriteBufferFromBlobStorage.h>


namespace DB
{

// TODO: abstract this function from DiskS3.cpp, from where it was copy-pasted
String getRandomName(char first = 'a', char last = 'z', size_t len = 64)
{
    std::uniform_int_distribution<int> distribution(first, last);
    String res(len, ' ');
    for (auto & c : res)
        c = distribution(thread_local_rng);
    return res;
}


WriteBufferFromBlobStorage::WriteBufferFromBlobStorage(
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
    const String & blob_path_,
    UInt64 max_single_part_upload_size_,
    size_t buf_size_) :
    BufferWithOwnMemory<WriteBuffer>(buf_size_, nullptr, 0),
    blob_container_client(blob_container_client_),
    max_single_part_upload_size(max_single_part_upload_size_),
    blob_path(blob_path_) {}


void WriteBufferFromBlobStorage::nextImpl()
{
    if (!offset())
        return;

    auto * pos = working_buffer.begin();
    auto len = offset();
    auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);

    size_t read = 0;
    while (read < len)
    {
        auto part_len = std::min(len - read, max_single_part_upload_size);

        auto block_id = getRandomName();
        block_ids.push_back(block_id);

        Azure::Core::IO::MemoryBodyStream tmp_buffer(reinterpret_cast<uint8_t *>(pos + read), part_len);
        block_blob_client.StageBlock(block_id, tmp_buffer);

        read += part_len;
    }
}

void WriteBufferFromBlobStorage::finalize()
{
    if (finalized)
        return;

    next();

    auto block_blob_client = blob_container_client->GetBlockBlobClient(blob_path);
    block_blob_client.CommitBlockList(block_ids);
    finalized = true;
}

}

#endif
