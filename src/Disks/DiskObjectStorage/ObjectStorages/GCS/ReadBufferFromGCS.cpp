#include <Disks/DiskObjectStorage/ObjectStorages/GCS/ReadBufferFromGCS.h>

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSCommon.h>
#include <Common/Throttler.h>
#include <Common/logger_useful.h>

namespace gcs = ::google::cloud::storage;

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}

ReadBufferFromGCS::ReadBufferFromGCS(
    std::shared_ptr<gcs::Client> client_,
    const String & bucket_,
    const String & key_,
    const ReadSettings & read_settings_,
    bool use_external_buffer_,
    size_t offset_,
    size_t read_until_position_,
    bool restricted_seek_,
    std::optional<size_t> file_size_)
    : ReadBufferFromFileBase()
    , client(std::move(client_))
    , bucket(bucket_)
    , key(key_)
    , read_settings(read_settings_)
    , use_external_buffer(use_external_buffer_)
    , restricted_seek(restricted_seek_)
    , offset(offset_)
    , read_until_position(read_until_position_)
    , tmp_buffer_size(read_settings_.remote_fs_settings.buffer_size)
{
    file_size = file_size_;
    if (!use_external_buffer)
    {
        tmp_buffer.resize(tmp_buffer_size);
        data_ptr = tmp_buffer.data();
        data_capacity = tmp_buffer_size;
    }
}

void ReadBufferFromGCS::initialize()
{
    if (initialized)
        return;

    /// GCS ReadRange is right-open [begin, end), which matches read_until_position (exclusive).
    if (read_until_position)
    {
        if (static_cast<off_t>(read_until_position) < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Attempt to read beyond the right offset ({} > {})", offset, read_until_position - 1);

        read_stream = std::make_unique<gcs::ObjectReadStream>(
            client->ReadObject(bucket, key, gcs::ReadRange(offset, read_until_position)));
    }
    else
    {
        read_stream = std::make_unique<gcs::ObjectReadStream>(
            client->ReadObject(bucket, key, gcs::ReadFromOffset(offset)));
    }

    if (!read_stream->status().ok())
        throwFromGCSStatus(read_stream->status(),
            fmt::format("while opening a read stream for '{}' in bucket '{}' at offset {}", key, bucket, offset));

    initialized = true;
}

bool ReadBufferFromGCS::nextImpl()
{
    if (read_until_position)
    {
        if (read_until_position == offset)
            return false;

        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Attempt to read beyond the right offset ({} > {})", offset, read_until_position - 1);
    }

    if (!initialized)
        initialize();

    if (use_external_buffer)
    {
        data_ptr = internal_buffer.begin();
        data_capacity = internal_buffer.size();
    }

    size_t to_read = data_capacity;
    if (read_until_position)
        to_read = std::min(to_read, static_cast<size_t>(read_until_position - offset));

    read_stream->read(data_ptr, static_cast<std::streamsize>(to_read));
    const size_t bytes_read = static_cast<size_t>(read_stream->gcount());

    if (bytes_read == 0)
    {
        /// The read produced no bytes: either clean EOF (status stays ok) or a transport error.
        if (!read_stream->status().ok())
            throwFromGCSStatus(read_stream->status(),
                fmt::format("while reading '{}' in bucket '{}' at offset {}", key, bucket, offset));
        return false;
    }

    BufferBase::set(data_ptr, bytes_read, 0);
    offset += bytes_read;

    if (read_settings.remote_throttler)
        read_settings.remote_throttler->throttle(bytes_read);

    return true;
}

off_t ReadBufferFromGCS::seek(off_t offset_, int whence)
{
    if (offset_ == getPosition() && whence == SEEK_SET)
        return offset_;

    if (initialized && restricted_seek)
        throw Exception(
            ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
            "Seek is allowed only before the first read attempt from the buffer (current offset: "
            "{}, new offset: {}, reading until position: {}, available: {})",
            getPosition(), offset_, read_until_position, available());

    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset_);

    if (!restricted_seek)
    {
        /// Seek within the already-buffered data.
        if (!working_buffer.empty()
            && static_cast<size_t>(offset_) >= offset - working_buffer.size()
            && offset_ < offset)
        {
            pos = working_buffer.end() - (offset - offset_);
            return getPosition();
        }

        resetWorkingBuffer();
        read_stream.reset();
        initialized = false;
    }

    offset = offset_;
    return offset;
}

off_t ReadBufferFromGCS::getPosition()
{
    return offset - available();
}

void ReadBufferFromGCS::setReadUntilPosition(size_t position)
{
    if (static_cast<off_t>(position) != read_until_position)
    {
        read_until_position = position;
        resetWorkingBuffer();
        read_stream.reset();
        initialized = false;
    }
}

void ReadBufferFromGCS::setReadUntilEnd()
{
    if (read_until_position)
    {
        read_until_position = 0;
        resetWorkingBuffer();
        read_stream.reset();
        initialized = false;
    }
}

std::optional<size_t> ReadBufferFromGCS::tryGetFileSize()
{
    if (file_size)
        return file_size;

    auto metadata = client->GetObjectMetadata(bucket, key);
    if (!metadata)
        return std::nullopt;

    file_size = metadata->size();
    return file_size;
}

}

#endif
