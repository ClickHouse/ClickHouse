#include <Disks/DiskObjectStorage/ObjectStorages/GCS/ReadBufferFromGCS.h>

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSCommon.h>
#include <Common/BlobStorageLogWriter.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Stopwatch.h>
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
    extern const int S3_OBJECT_CHANGED_DURING_READ;
}

namespace
{
    [[noreturn]] void throwReadFailure(
        const google::cloud::Status & status,
        const String & bucket,
        const String & key,
        const std::optional<Int64> & expected_generation,
        const String & context)
    {
        if (expected_generation && status.code() == google::cloud::StatusCode::kFailedPrecondition)
            throw Exception(
                ErrorCodes::S3_OBJECT_CHANGED_DURING_READ,
                "GCS object {}/{} was replaced during read (IfGenerationMatch on generation {} failed); "
                "retry the query, or set s3_validate_etag_on_read=0 to disable this check",
                bucket,
                key,
                *expected_generation);

        throwFromGCSStatus(status, context);
    }

    void logGCSReadFailure(
        const BlobStorageLogWriterPtr & blob_storage_log,
        const String & bucket,
        const String & key,
        size_t elapsed_microseconds,
        const google::cloud::Status & status)
    {
        if (!blob_storage_log)
            return;

        try
        {
            blob_storage_log->addEvent(
                BlobStorageLogElement::EventType::Read,
                bucket, key, /* local_path */ {},
                /* data_size */ 0,
                elapsed_microseconds,
                static_cast<Int32>(status.code()), status.message());
        }
        catch (...)
        {
            tryLogCurrentException("ReadBufferFromGCS");
        }
    }
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
    std::optional<size_t> file_size_,
    std::optional<Int64> expected_generation_,
    BlobStorageLogWriterPtr blob_storage_log_)
    : ReadBufferFromFileBase()
    , client(std::move(client_))
    , bucket(bucket_)
    , key(key_)
    , read_settings(read_settings_)
    , use_external_buffer(use_external_buffer_)
    , restricted_seek(restricted_seek_)
    , offset(offset_)
    , read_until_position(read_until_position_)
    , expected_generation(expected_generation_)
    , blob_storage_log(std::move(blob_storage_log_))
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

ReadBufferFromGCS::~ReadBufferFromGCS()
{
    /// A failure already logged its own event at the point of detection (see `initialize` /
    /// `nextImpl`); only a fully successful lifetime is aggregated here. The destructor is
    /// implicitly `noexcept`, so wrap the potentially-throwing `addEvent` (allocations inside
    /// `SystemLogQueue::push`) in `try/catch` to avoid `std::terminate` if it throws during
    /// stack unwinding.
    if (blob_storage_log && read_attempted && !read_failed)
    {
        try
        {
            blob_storage_log->addEvent(
                BlobStorageLogElement::EventType::Read,
                bucket, key, /* local_path */ {},
                total_bytes_read,
                total_read_microseconds,
                /* error_code */ 0, /* error_message */ {});
        }
        catch (...)
        {
            tryLogCurrentException("ReadBufferFromGCS");
        }
    }
}

void ReadBufferFromGCS::initialize()
{
    if (initialized)
        return;

    /// A default-constructed option is "not set" and does not affect the request.
    gcs::IfGenerationMatch generation_match;
    if (expected_generation)
        generation_match = gcs::IfGenerationMatch(*expected_generation);

    Stopwatch watch;
    ResourceGuard rlock(ResourceGuard::Metrics::getIORead(), read_settings.io_scheduling.read_resource_link, data_capacity);
    /// GCS ReadRange is right-open [begin, end), which matches read_until_position (exclusive).
    if (read_until_position)
    {
        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Attempt to read beyond the right offset ({} > {})", offset, read_until_position - 1);

        read_stream = std::make_unique<gcs::ObjectReadStream>(
            client->ReadObject(bucket, key, gcs::ReadRange(offset, read_until_position), generation_match));
    }
    else
    {
        read_stream = std::make_unique<gcs::ObjectReadStream>(
            client->ReadObject(bucket, key, gcs::ReadFromOffset(offset), generation_match));
    }
    const size_t elapsed_microseconds = watch.elapsedMicroseconds();

    if (!read_stream->status().ok())
    {
        read_failed = true;
        logGCSReadFailure(blob_storage_log, bucket, key, elapsed_microseconds, read_stream->status());
        throwReadFailure(read_stream->status(), bucket, key, expected_generation,
            fmt::format("while opening a read stream for '{}' in bucket '{}' at offset {}{}", key, bucket, offset,
                expected_generation
                    ? fmt::format(" (pinned to generation {}; a precondition failure means the object was overwritten during the read)",
                        *expected_generation)
                    : ""));
    }

    total_read_microseconds += elapsed_microseconds;
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

    read_attempted = true;

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

    Stopwatch watch;
    ResourceGuard rlock(ResourceGuard::Metrics::getIORead(), read_settings.io_scheduling.read_resource_link, to_read);
    read_stream->read(data_ptr, static_cast<std::streamsize>(to_read));
    const size_t elapsed_microseconds = watch.elapsedMicroseconds();
    const size_t bytes_read = static_cast<size_t>(read_stream->gcount());

    if (bytes_read == 0)
    {
        /// The read produced no bytes: either clean EOF (status stays ok) or a transport error.
        if (!read_stream->status().ok())
        {
            read_failed = true;
            logGCSReadFailure(blob_storage_log, bucket, key, elapsed_microseconds, read_stream->status());
            throwReadFailure(
                read_stream->status(),
                bucket,
                key,
                expected_generation,
                fmt::format("while reading '{}' in bucket '{}' at offset {}", key, bucket, offset));
        }
        return false;
    }

    BufferBase::set(data_ptr, bytes_read, 0);
    offset += bytes_read;
    total_bytes_read += bytes_read;
    total_read_microseconds += elapsed_microseconds;

    rlock.unlock(bytes_read);

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
