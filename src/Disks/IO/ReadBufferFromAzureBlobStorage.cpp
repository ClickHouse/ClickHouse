#include <Common/config.h>

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <IO/ReadBufferFromString.h>
#include <Common/logger_useful.h>
#include <Common/Throttler.h>
#include <base/sleep.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int RECEIVED_EMPTY_DATA;
    extern const int LOGICAL_ERROR;
}


ReadBufferFromAzureBlobStorage::ReadBufferFromAzureBlobStorage(
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
    const String & path_,
    const ReadSettings & read_settings_,
    size_t max_single_read_retries_,
    size_t max_single_download_retries_,
    bool use_external_buffer_,
    size_t read_until_position_)
    : ReadBufferFromFileBase(read_settings_.remote_fs_buffer_size, nullptr, 0)
    , blob_container_client(blob_container_client_)
    , path(path_)
    , max_single_read_retries(max_single_read_retries_)
    , max_single_download_retries(max_single_download_retries_)
    , read_settings(read_settings_)
    , tmp_buffer_size(read_settings.remote_fs_buffer_size)
    , use_external_buffer(use_external_buffer_)
    , read_until_position(read_until_position_)
{
    if (!use_external_buffer)
    {
        tmp_buffer.resize(tmp_buffer_size);
        data_ptr = tmp_buffer.data();
        data_capacity = tmp_buffer_size;
    }
}

SeekableReadBuffer::Range ReadBufferFromAzureBlobStorage::getRemainingReadRange() const
{
    return Range{
        .left = static_cast<size_t>(offset),
        .right = read_until_position ? std::optional{read_until_position - 1} : std::nullopt
    };
}

void ReadBufferFromAzureBlobStorage::setReadUntilPosition(size_t position)
{
    read_until_position = position;
    initialized = false;
}

bool ReadBufferFromAzureBlobStorage::nextImpl()
{
    if (read_until_position)
    {
        if (read_until_position == offset)
            return false;

        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);
    }

    if (!initialized)
        initialize();

    if (use_external_buffer)
    {
        data_ptr = internal_buffer.begin();
        data_capacity = internal_buffer.size();
    }

    size_t to_read_bytes = std::min(static_cast<size_t>(total_size - offset), data_capacity);
    size_t bytes_read = 0;

    size_t sleep_time_with_backoff_milliseconds = 100;
    for (size_t i = 0; i < max_single_read_retries; ++i)
    {
        try
        {
            bytes_read = data_stream->ReadToCount(reinterpret_cast<uint8_t *>(data_ptr), to_read_bytes);
            if (read_settings.remote_throttler)
                read_settings.remote_throttler->add(bytes_read);
            break;
        }
        catch (const Azure::Storage::StorageException & e)
        {
            LOG_INFO(log, "Exception caught during Azure Read for file {} at attempt {}: {}", path, i, e.Message);
            if (i + 1 == max_single_read_retries)
                throw;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
            initialized = false;
            initialize();
        }
    }

    if (bytes_read == 0)
        return false;

    BufferBase::set(data_ptr, bytes_read, 0);
    offset += bytes_read;

    return true;
}


off_t ReadBufferFromAzureBlobStorage::seek(off_t offset_, int whence)
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


off_t ReadBufferFromAzureBlobStorage::getPosition()
{
    return offset - available();
}


void ReadBufferFromAzureBlobStorage::initialize()
{
    if (initialized)
        return;

    Azure::Storage::Blobs::DownloadBlobOptions download_options;

    Azure::Nullable<int64_t> length {};
    if (read_until_position != 0)
        length = {static_cast<int64_t>(read_until_position - offset)};

    download_options.Range = {static_cast<int64_t>(offset), length};

    blob_client = std::make_unique<Azure::Storage::Blobs::BlobClient>(blob_container_client->GetBlobClient(path));

    size_t sleep_time_with_backoff_milliseconds = 100;
    for (size_t i = 0; i < max_single_download_retries; ++i)
    {
        try
        {
            auto download_response = blob_client->Download(download_options);
            data_stream = std::move(download_response.Value.BodyStream);
            break;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            LOG_INFO(log, "Exception caught during Azure Download for file {} at offset {} at attempt {} : {}", path, offset, i + 1, e.Message);
            if (i + 1 == max_single_download_retries)
                throw;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
    }

    if (data_stream == nullptr)
        throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "Null data stream obtained while downloading file {} from Blob Storage", path);

    total_size = data_stream->Length() + offset;

    initialized = true;
}

}

#endif
