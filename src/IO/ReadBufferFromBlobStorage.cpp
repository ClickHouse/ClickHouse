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
    extern const int RECEIVED_EMPTY_DATA;
}


ReadBufferFromBlobStorage::ReadBufferFromBlobStorage(
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
    const String & path_,
    size_t buf_size_) :
    SeekableReadBuffer(nullptr, 0),
    blob_container_client(blob_container_client_),
    tmp_buffer(buf_size_),
    path(path_),
    buf_size(buf_size_) {}


bool ReadBufferFromBlobStorage::nextImpl()
{
    // TODO: is this "stream" approach better than a single DownloadTo approach (last commit 90fc230c4dfacc1a9d50d2d65b91363150caa784) ?

    if (!initialized)
        initialize();

    if (offset >= total_size)
        return false;

    size_t to_read_bytes = std::min(total_size - offset, buf_size);
    size_t bytes_read = 0;

    auto sleep_time_with_backoff_milliseconds = std::chrono::milliseconds(100);
    for (int i = 0; i < 3; i++)
    {
        try
        {
            bytes_read = data_stream->ReadToCount(tmp_buffer.data(), to_read_bytes);
            break;
        }
        catch (const Azure::Storage::StorageException & e)
        {
            LOG_INFO(log, "Exception caught during Azure Read for file {} : {}", path, e.Message);

            std::this_thread::sleep_for(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
            initialized = false;
            initialize();
        }
    }

    if (bytes_read == 0)
        return false;

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
    // TODO: which one is the right one? In S3: return offset - available();

    return offset;
}


void ReadBufferFromBlobStorage::initialize()
{
    if (initialized)
        return;

    Azure::Storage::Blobs::DownloadBlobOptions download_options;
    if (offset != 0)
        download_options.Range = {static_cast<int64_t>(offset), {}};

    blob_client = std::make_unique<Azure::Storage::Blobs::BlobClient>(blob_container_client->GetBlobClient(path));

    try
    {
        auto download_response = blob_client->Download(download_options);
        data_stream = std::move(download_response.Value.BodyStream);
    }
    catch (const Azure::Storage::StorageException & e)
    {
        // TODO log the download options to
        LOG_INFO(log, "Exception caught during Azure Download for file {} : {}", path, e.Message);
        throw e;
    }


    if (data_stream == nullptr)
        // TODO log the download options and the context
        throw Exception("Null data stream obtained while downloading a file from Blob Storage", ErrorCodes::RECEIVED_EMPTY_DATA);

    total_size = data_stream->Length() + offset;

    initialized = true;
}

}

#endif
