#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <iostream>

#include <IO/ReadBufferFromBlobStorage.h>
#include <IO/ReadBufferFromString.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


ReadBufferFromBlobStorage::ReadBufferFromBlobStorage(
    Azure::Storage::Blobs::BlobContainerClient blob_container_client_,
    const String & path_,
    UInt64 max_single_read_retries_,
    size_t buf_size_) :
    SeekableReadBuffer(nullptr, 0),
    blob_container_client(blob_container_client_),
    tmp_buffer(buf_size_),
    max_single_read_retries(max_single_read_retries_),
    path(path_),
    buf_size(buf_size_) {}


bool ReadBufferFromBlobStorage::nextImpl()
{
    // TODO: is this "stream" approach better than a single DownloadTo approach (last commit 90fc230c4dfacc1a9d50d2d65b91363150caa784) ?

    if (!initialized)
        initialize();

    if (static_cast<size_t>(offset) >= total_size)
        return false;

    size_t to_read_bytes = std::min(total_size - offset, buf_size);

    size_t bytes_read = data_stream->Read(tmp_buffer.data(), to_read_bytes);

    BufferBase::set(reinterpret_cast<char *>(tmp_buffer.data()), bytes_read, 0);

    offset += bytes_read;

    return true;
}


off_t ReadBufferFromBlobStorage::seek(off_t offset_, int whence)
{
    if (initialized)
        throw Exception("Seek is allowed only before first read attempt from the buffer.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (whence != SEEK_SET)
        throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (offset_ < 0)
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset_), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    offset = offset_;

    return offset;
}


off_t ReadBufferFromBlobStorage::getPosition()
{
    // TODO: which one is the right one?
    // return offset - available();

    return offset;
}


void ReadBufferFromBlobStorage::initialize()
{
    if (initialized)
        return;

    auto blob_client = blob_container_client.GetBlobClient(path);

    auto download_response = blob_client.Download();

    data_stream = std::move(download_response.Value.BodyStream);

    if (data_stream == nullptr)
    {
        // TODO: change error code
        throw Exception("Null data stream obtained from blob Download", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    }

    total_size = data_stream->Length();

    if (offset != 0)
    {
        // TODO: is it the right way?
        /// try to rewind to offset in the buffer
        size_t total_read_bytes = 0;
        while (total_read_bytes < static_cast<size_t>(offset))
        {
            size_t to_read_bytes = std::min(offset - total_read_bytes, buf_size);
            size_t bytes_read = data_stream->Read(tmp_buffer.data(), to_read_bytes);
            total_read_bytes += bytes_read;
        }
    }

    initialized = true;

    // TODO: dummy if to avoid warning for max_single_read_retries
    if (max_single_read_retries == 0)
        return;
}

}

#endif
