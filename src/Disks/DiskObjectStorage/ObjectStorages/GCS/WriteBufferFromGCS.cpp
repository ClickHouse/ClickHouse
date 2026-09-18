#include <Disks/DiskObjectStorage/ObjectStorages/GCS/WriteBufferFromGCS.h>

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSCommon.h>
#include <Common/CurrentThread.h>
#include <IO/ReadHelpers.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <google/cloud/storage/object_metadata.h>

namespace gcs = ::google::cloud::storage;

namespace ProfileEvents
{
    extern const Event GCSWriteObject;
    extern const Event DiskGCSWriteObject;
    extern const Event WriteBufferFromGCSMicroseconds;
    extern const Event WriteBufferFromGCSBytes;
    extern const Event WriteBufferFromGCSRequestsErrors;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// A conditional write is a compare-and-swap request; performing an unconditional write instead
/// would silently discard one of two concurrent writers. GCS expresses both halves through
/// generation preconditions: `IfGenerationMatch(0)` succeeds only if the object does not exist
/// (If-None-Match: `*`), and `IfGenerationMatch(generation)` only if the live generation matches
/// (If-Match, since this backend's etag *is* the generation — see `toObjectMetadata`).
/// A default-constructed option is not set, so the unconditional path stays a single code path.
gcs::IfGenerationMatch makeWritePrecondition(const WriteSettings & write_settings, const String & bucket, const String & key)
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

}

WriteBufferFromGCS::WriteBufferFromGCS(
    std::shared_ptr<gcs::Client> client_,
    const String & bucket_,
    const String & key_,
    size_t buf_size_,
    const WriteSettings & write_settings_,
    BlobStorageLogWriterPtr blob_log_,
    std::optional<ObjectAttributes> attributes_,
    bool for_disk_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , client(std::move(client_))
    , bucket(bucket_)
    , key(key_)
    , write_settings(write_settings_)
    , blob_log(std::move(blob_log_))
    , attributes(std::move(attributes_))
    , for_disk(for_disk_)
{
    ProfileEvents::increment(ProfileEvents::GCSWriteObject);
    if (for_disk)
        ProfileEvents::increment(ProfileEvents::DiskGCSWriteObject);

    auto precondition = makeWritePrecondition(write_settings, bucket, key);

    if (attributes && !attributes->empty())
    {
        gcs::ObjectMetadata object_metadata;
        for (const auto & [name, value] : *attributes)
            object_metadata.upsert_metadata(name, value);
        write_stream = std::make_unique<gcs::ObjectWriteStream>(
            client->WriteObject(bucket, key, gcs::WithObjectMetadata(std::move(object_metadata)), precondition));
    }
    else
    {
        write_stream = std::make_unique<gcs::ObjectWriteStream>(client->WriteObject(bucket, key, precondition));
    }
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

void WriteBufferFromGCS::nextImpl()
{
    const size_t bytes_to_write = offset();
    if (bytes_to_write == 0)
        return;

    CurrentThread::IOSchedulingScope io_scope(write_settings.io_scheduling);
    Stopwatch watch;
    write_stream->write(working_buffer.begin(), static_cast<std::streamsize>(bytes_to_write));
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

    ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSBytes, bytes_to_write);
    total_bytes_written += bytes_to_write;

    if (write_settings.remote_throttler)
        write_settings.remote_throttler->throttle(bytes_to_write);
}

void WriteBufferFromGCS::finalizeImpl()
{
    /// Flush whatever remains in the working buffer, then close the upload.
    next();

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
    logUploadResult(result ? 0 : static_cast<Int32>(result.status().code()), result ? "" : result.status().message());

    if (!result)
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSRequestsErrors);
        throwFromGCSStatus(result.status(),
            fmt::format("while finalizing the upload of '{}' in bucket '{}'", key, bucket));
    }
}

void WriteBufferFromGCS::cancelImpl() noexcept
{
    /// Abandon the (possibly resumable) upload without finalizing it. Dropping the stream leaves any
    /// resumable session unfinished; GCS garbage-collects incomplete uploads automatically.
    write_stream.reset();
}

}

#endif
