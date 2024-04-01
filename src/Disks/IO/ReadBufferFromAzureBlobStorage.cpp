#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <IO/ReadBufferFromString.h>
#include <Common/logger_useful.h>
#include <Common/Throttler.h>
#include <base/sleep.h>
#include <Common/ProfileEvents.h>
#include <IO/SeekableReadBuffer.h>

namespace ProfileEvents
{
    extern const Event RemoteReadThrottlerBytes;
    extern const Event RemoteReadThrottlerSleepMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}

ReadBufferFromAzureBlobStorage::ReadBufferFromAzureBlobStorage(
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
    const String & path_,
    const ReadSettings & read_settings_,
    size_t max_single_download_retries_,
    bool use_external_buffer_,
    bool restricted_seek_,
    size_t read_until_position_)
    : ReadBufferFromFileBase(use_external_buffer_ ? 0 : read_settings_.remote_fs_buffer_size, nullptr, 0)
    , blob_container_client(blob_container_client_)
    , path(path_)
    , max_single_download_retries(max_single_download_retries_)
    , read_settings(read_settings_)
    , tmp_buffer_size(read_settings.remote_fs_buffer_size)
    , use_external_buffer(use_external_buffer_)
    , restricted_seek(restricted_seek_)
    , read_until_position(read_until_position_)
{
    if (!use_external_buffer)
    {
        tmp_buffer.resize(tmp_buffer_size);
        data_ptr = tmp_buffer.data();
        data_capacity = tmp_buffer_size;
    }
}

void ReadBufferFromAzureBlobStorage::setReadUntilEnd()
{
    if (read_until_position)
    {
        read_until_position = 0;
        if (initialized)
        {
            offset = getPosition();
            resetWorkingBuffer();
            initialized = false;
        }
    }

}

void ReadBufferFromAzureBlobStorage::setReadUntilPosition(size_t position)
{
    read_until_position = position;
    initialized = false;
}

bool ReadBufferFromAzureBlobStorage::nextImpl()
{
    size_t bytes_read = 0;
    if (read_until_position)
    {
        if (read_until_position == offset)
            return false;

        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);
    }

    if (use_external_buffer)
    {
        data_ptr = internal_buffer.begin();
        data_capacity = internal_buffer.size();
    }

    bytes_read = readBytes();

    if (bytes_read == 0)
        return false;

    BufferBase::set(data_ptr, bytes_read, 0);
    offset += bytes_read;

    return true;
}

off_t ReadBufferFromAzureBlobStorage::seek(off_t offset_, int whence)
{
    if (offset_ == getPosition() && whence == SEEK_SET)
        return offset_;

    if (initialized && restricted_seek)
    {
        throw Exception(
            ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
            "Seek is allowed only before first read attempt from the buffer (current offset: "
            "{}, new offset: {}, reading until position: {}, available: {})",
            getPosition(), offset_, read_until_position, available());
    }

    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset_);

    if (!restricted_seek)
    {
        if (!working_buffer.empty()
            && static_cast<size_t>(offset_) >= offset - working_buffer.size()
            && offset_ < offset)
        {
            pos = working_buffer.end() - (offset - offset_);
            assert(pos >= working_buffer.begin());
            assert(pos < working_buffer.end());

            return getPosition();
        }

        off_t position = getPosition();
        if (initialized && offset_ > position)
        {
            size_t diff = offset_ - position;
            if (diff < read_settings.remote_read_min_bytes_for_seek)
            {
                ignore(diff);
                return offset_;
            }
        }

        resetWorkingBuffer();
        if (initialized)
            initialized = false;
    }

    offset = offset_;
    return offset;
}

off_t ReadBufferFromAzureBlobStorage::getPosition()
{
    return offset - available();
}

size_t ReadBufferFromAzureBlobStorage::readBytes()
{
    Azure::Storage::Blobs::DownloadBlobToOptions download_options;

    Azure::Nullable<int64_t> length {};

    /// 0 means read full file
    int64_t to_read_bytes = 0;
    if (read_until_position != 0)
    {
        to_read_bytes = read_until_position - offset;
        to_read_bytes = std::min(to_read_bytes, static_cast<int64_t>(data_capacity));
    }

    if (!to_read_bytes && (getFileSize() > data_capacity))
        to_read_bytes = data_capacity;

    if (to_read_bytes)
        length = {static_cast<int64_t>(to_read_bytes)};
    download_options.Range = {static_cast<int64_t>(offset), length};
    LOG_INFO(log, "Read bytes range is offset = {} and to_read_bytes = {}", offset, to_read_bytes);


    if (!blob_client)
        blob_client = std::make_unique<Azure::Storage::Blobs::BlobClient>(blob_container_client->GetBlobClient(path));

    size_t sleep_time_with_backoff_milliseconds = 100;

    auto handle_exception = [&, this](const auto & e, size_t i)
    {
        LOG_DEBUG(log, "Exception caught during Azure Download for file {} at offset {} at attempt {}/{}: {}", path, offset, i + 1, max_single_download_retries, e.Message);
        if (i + 1 == max_single_download_retries)
            throw;

        sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
        sleep_time_with_backoff_milliseconds *= 2;
    };

    long read_bytes = 0;

    for (size_t i = 0; i < max_single_download_retries; ++i)
    {
        try
        {
            auto download_response = blob_client->DownloadTo(reinterpret_cast<uint8_t *>(data_ptr), data_capacity, download_options);
            read_bytes = download_response.Value.ContentRange.Length.Value();

            if (read_settings.remote_throttler)
                read_settings.remote_throttler->add(read_bytes, ProfileEvents::RemoteReadThrottlerBytes, ProfileEvents::RemoteReadThrottlerSleepMicroseconds);

            break;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            handle_exception(e,i);
        }
    }

    initialized = true;
    return read_bytes;
}

size_t ReadBufferFromAzureBlobStorage::getFileSize()
{
    if (!blob_client)
        blob_client = std::make_unique<Azure::Storage::Blobs::BlobClient>(blob_container_client->GetBlobClient(path));

    if (file_size.has_value())
        return *file_size;

    file_size = blob_client->GetProperties().Value.BlobSize;
    return *file_size;
}

size_t ReadBufferFromAzureBlobStorage::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & /*progress_callback*/) const
{
    size_t initial_n = n;

    size_t sleep_time_with_backoff_milliseconds = 100;

    for (size_t i = 0; i < max_single_download_retries && n > 0; ++i)
    {
        size_t bytes_copied = 0;
        try
        {
            Azure::Storage::Blobs::DownloadBlobToOptions download_options;
            download_options.Range = {static_cast<int64_t>(range_begin), n};
            auto download_response = blob_client->DownloadTo(reinterpret_cast<uint8_t *>(to), n, download_options);
            bytes_copied = download_response.Value.ContentRange.Length.Value();

            LOG_TEST(log, "AzureBlobStorage readBigAt read bytes {}", bytes_copied);

            if (read_settings.remote_throttler)
                read_settings.remote_throttler->add(bytes_copied, ProfileEvents::RemoteReadThrottlerBytes, ProfileEvents::RemoteReadThrottlerSleepMicroseconds);
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            LOG_DEBUG(log, "Exception caught during Azure Download for file {} at offset {} at attempt {}/{}: {}", path, offset, i + 1, max_single_download_retries, e.Message);
            if (i + 1 == max_single_download_retries)
                throw;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }

        range_begin += bytes_copied;
        to += bytes_copied;
        n -= bytes_copied;
    }

    return initial_n;
}

}

#endif
