#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <IO/WriteBufferFromBlobStorage.h>


namespace DB
{

WriteBufferFromBlobStorage::WriteBufferFromBlobStorage(
    Azure::Storage::Blobs::BlobContainerClient blob_container_client_,
    const String & blob_path_,
    size_t buf_size_) :
    BufferWithOwnMemory<WriteBuffer>(buf_size_, nullptr, 0),
    blob_container_client(blob_container_client_),
    blob_path(blob_path_),
    buf_size(buf_size_)
{
    // allocateBuffer();
}

void WriteBufferFromBlobStorage::allocateBuffer()
{

}

void WriteBufferFromBlobStorage::nextImpl() {
    std::cout << "buf_size: " << buf_size << "\n";
    // std::cout << "WriteBufferFromBlobStorage:nextImpl\n\n\n";

    if (!offset())
        return;

    Azure::Core::IO::MemoryBodyStream tmp_buffer(reinterpret_cast<uint8_t *>(position()), offset());

    blob_container_client.UploadBlob(blob_path, tmp_buffer);
}

}

#endif
