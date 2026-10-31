#include <Disks/DiskObjectStorage/ObjectStorages/GCS/WriteBufferFromGCS.h>

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSCommon.h>
#include <Common/CurrentThread.h>
#include <IO/ReadHelpers.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <google/cloud/storage/auto_finalize.h>
#include <google/cloud/storage/object_metadata.h>

namespace gcs = ::google::cloud::storage;

namespace ProfileEvents
{
    extern const Event GCSWriteObject;
    extern const Event DiskGCSWriteObject;
    extern const Event GCSInsertObject;
    extern const Event DiskGCSInsertObject;
    extern const Event WriteBufferFromGCSMicroseconds;
    extern const Event WriteBufferFromGCSBytes;
    extern const Event WriteBufferFromGCSRequestsErrors;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CHECKSUM_DOESNT_MATCH;
}

gcs::IfGenerationMatch makeGCSWritePrecondition(const WriteSettings & write_settings, const String & bucket, const String & key)
{
    const auto & if_none_match = write_settings.object_storage_write_if_none_match;
    const auto & if_match = write_settings.object_storage_write_if_match;

    if (!if_none_match.empty() && !if_match.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "If-None-Match and If-Match cannot be used together for object '{}' in bucket '{}'", key, bucket);

    if (!if_none_match.empty())
    {
        if (if_none_match != "*")
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Native GCS supports only `*` for If-None-Match, got `{}`", if_none_match);
        return gcs::IfGenerationMatch(0);
    }

    if (!if_match.empty())
    {
        UInt64 generation = 0;
        if (!tryParse(generation, if_match))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Native GCS write of '{}' got If-Match etag '{}' which is not an object generation", key, if_match);
        return gcs::IfGenerationMatch(generation);
    }

    return {};
}

WriteBufferFromGCS::WriteBufferFromGCS(
    std::shared_ptr<gcs::Client> client_,
    const String & bucket_,
    const String & key_,
    size_t buf_size_,
    size_t max_single_part_upload_size_,
    const WriteSettings & write_settings_,
    BlobStorageLogWriterPtr blob_log_,
    std::optional<ObjectAttributes> attributes_,
    bool for_disk_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , client(std::move(client_))
    , bucket(bucket_)
    , key(key_)
    , max_single_part_upload_size(max_single_part_upload_size_)
    , write_settings(write_settings_)
    , precondition(makeGCSWritePrecondition(write_settings, bucket, key))
    , blob_log(std::move(blob_log_))
    , attributes(std::move(attributes_))
    , for_disk(for_disk_)
{
}

WriteBufferFromGCS::~WriteBufferFromGCS()
{
    if (!isFinalized() && !isCanceled())
        cancel();
}

void WriteBufferFromGCS::logUploadResult(Int32 error_code, const String & error_message)
{
    if (!blob_log || upload_result_logged)
        return;

    blob_log->addEvent(
        BlobStorageLogElement::EventType::Upload,
        bucket,
        key,
        /* local_path_ */ {},
        total_bytes_written,
        total_time_microseconds,
        error_code,
        error_message);
    upload_result_logged = true;
}

void WriteBufferFromGCS::accountUploadedBytes(size_t size)
{
    ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSBytes, size);
    total_bytes_written += size;

    if (write_settings.remote_throttler)
        write_settings.remote_throttler->throttle(size);
}

void WriteBufferFromGCS::openWriteStream()
{
    ProfileEvents::increment(ProfileEvents::GCSWriteObject);
    if (for_disk)
        ProfileEvents::increment(ProfileEvents::DiskGCSWriteObject);

    /// By default the destructor of `ObjectWriteStream` finalizes the upload, which would commit a
    /// truncated object when the buffer is canceled. The upload is finalized only by an explicit `Close`.
    const auto auto_finalize = gcs::AutoFinalize(gcs::AutoFinalizeConfig::kDisabled);
    if (attributes && !attributes->empty())
    {
        gcs::ObjectMetadata object_metadata;
        for (const auto & [name, value] : *attributes)
            object_metadata.upsert_metadata(name, value);
        write_stream = std::make_unique<gcs::ObjectWriteStream>(
            client->WriteObject(bucket, key, gcs::WithObjectMetadata(std::move(object_metadata)), precondition, auto_finalize));
    }
    else
    {
        write_stream = std::make_unique<gcs::ObjectWriteStream>(client->WriteObject(bucket, key, precondition, auto_finalize));
    }
}

void WriteBufferFromGCS::writeToStream(const char * data, size_t size)
{
    CurrentThread::IOSchedulingScope io_scope(write_settings.io_scheduling);
    Stopwatch watch;
    write_stream->write(data, static_cast<std::streamsize>(size));
    const size_t elapsed_microseconds = watch.elapsedMicroseconds();
    total_time_microseconds += elapsed_microseconds;
    ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSMicroseconds, elapsed_microseconds);

    if (!write_stream->good())
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSRequestsErrors);
        const auto & status = write_stream->last_status();
        logUploadResult(static_cast<Int32>(status.code()), status.message());
        throwFromGCSStatus(write_stream->last_status(),
            fmt::format("while writing '{}' in bucket '{}'", key, bucket));
    }

    accountUploadedBytes(size);
}

void WriteBufferFromGCS::nextImpl()
{
    const size_t bytes_to_write = offset();
    if (bytes_to_write == 0)
        return;

    if (!write_stream)
    {
        if (pending_data.size() + bytes_to_write <= max_single_part_upload_size)
        {
            pending_data.append(working_buffer.begin(), bytes_to_write);
            return;
        }

        /// The object is too large for a single request: hand everything accumulated so far to a
        /// resumable upload and stream the rest of the data through it.
        openWriteStream();
        if (!pending_data.empty())
        {
            writeToStream(pending_data.data(), pending_data.size());
            String().swap(pending_data);
        }
    }

    writeToStream(working_buffer.begin(), bytes_to_write);
}

void WriteBufferFromGCS::finalizeWriteStream()
{
    CurrentThread::IOSchedulingScope io_scope(write_settings.io_scheduling);
    Stopwatch watch;
    write_stream->Close();
    const size_t close_microseconds = watch.elapsedMicroseconds();
    total_time_microseconds += close_microseconds;
    ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSMicroseconds, close_microseconds);

    const auto & result = write_stream->metadata();

    /// Record the upload in `system.blob_storage_log` (like the S3 and Azure backends do), including
    /// the failed outcome. The stream is a single (internally resumable) upload, so one event covers
    /// the whole object.
    if (!result)
    {
        logUploadResult(static_cast<Int32>(result.status().code()), result.status().message());
        ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSRequestsErrors);
        throwFromGCSStatus(result.status(),
            fmt::format("while finalizing the upload of '{}' in bucket '{}'", key, bucket));
    }

    /// The server accepted the upload, but `Close` also compares the hash the client computed over the
    /// bytes it sent with the one the server reports for the stored object. A mismatch keeps the
    /// metadata and an OK `last_status` and only sets `badbit` on the stream, so it has to be checked
    /// separately: the stored object does not hold the data that was written.
    if (write_stream->bad())
    {
        const String message = fmt::format(
            "Checksum mismatch after the upload of '{}' in bucket '{}': computed {}, received {}",
            key, bucket, write_stream->computed_hash(), write_stream->received_hash());
        logUploadResult(ErrorCodes::CHECKSUM_DOESNT_MATCH, message);
        ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSRequestsErrors);
        throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH, "{}", message);
    }

    logUploadResult(0, "");
}

void WriteBufferFromGCS::insertObject()
{
    ProfileEvents::increment(ProfileEvents::GCSInsertObject);
    if (for_disk)
        ProfileEvents::increment(ProfileEvents::DiskGCSInsertObject);

    CurrentThread::IOSchedulingScope io_scope(write_settings.io_scheduling);
    Stopwatch watch;
    google::cloud::StatusOr<gcs::ObjectMetadata> result;
    if (attributes && !attributes->empty())
    {
        gcs::ObjectMetadata object_metadata;
        for (const auto & [name, value] : *attributes)
            object_metadata.upsert_metadata(name, value);
        result = client->InsertObject(bucket, key, pending_data, gcs::WithObjectMetadata(std::move(object_metadata)), precondition);
    }
    else
    {
        result = client->InsertObject(bucket, key, pending_data, precondition);
    }
    const size_t elapsed_microseconds = watch.elapsedMicroseconds();
    total_time_microseconds += elapsed_microseconds;
    ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSMicroseconds, elapsed_microseconds);

    if (result)
        accountUploadedBytes(pending_data.size());

    logUploadResult(result ? 0 : static_cast<Int32>(result.status().code()), result ? "" : result.status().message());

    if (!result)
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSRequestsErrors);
        throwFromGCSStatus(result.status(),
            fmt::format("while uploading '{}' in bucket '{}'", key, bucket));
    }

    String().swap(pending_data);
}

void WriteBufferFromGCS::finalizeImpl()
{
    /// Flush whatever remains in the working buffer, then complete the upload: a resumable one if the
    /// object outgrew a single request, otherwise one `InsertObject` request with the whole object.
    next();

    if (write_stream)
        finalizeWriteStream();
    else
        insertObject();
}

void WriteBufferFromGCS::cancelImpl() noexcept
{
    /// Abandon the (possibly resumable) upload without finalizing it. Dropping the stream leaves any
    /// resumable session unfinished; GCS garbage-collects incomplete uploads automatically.
    write_stream.reset();
    String().swap(pending_data);
}

}

#endif
