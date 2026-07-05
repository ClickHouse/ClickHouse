#include <Disks/DiskObjectStorage/ObjectStorages/GCS/WriteBufferFromGCS.h>

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSCommon.h>
#include <Common/logger_useful.h>

#include <google/cloud/storage/object_metadata.h>

namespace gcs = ::google::cloud::storage;

namespace DB
{

WriteBufferFromGCS::WriteBufferFromGCS(
    std::shared_ptr<gcs::Client> client_,
    const String & bucket_,
    const String & key_,
    size_t buf_size_,
    const WriteSettings &,
    std::optional<ObjectAttributes> attributes_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , client(std::move(client_))
    , bucket(bucket_)
    , key(key_)
    , attributes(std::move(attributes_))
{
    if (attributes && !attributes->empty())
    {
        gcs::ObjectMetadata object_metadata;
        for (const auto & [name, value] : *attributes)
            object_metadata.upsert_metadata(name, value);
        write_stream = std::make_unique<gcs::ObjectWriteStream>(
            client->WriteObject(bucket, key, gcs::WithObjectMetadata(std::move(object_metadata))));
    }
    else
    {
        write_stream = std::make_unique<gcs::ObjectWriteStream>(client->WriteObject(bucket, key));
    }
}

WriteBufferFromGCS::~WriteBufferFromGCS()
{
    if (!isFinalized() && !isCanceled())
        cancel();
}

void WriteBufferFromGCS::nextImpl()
{
    const size_t bytes_to_write = offset();
    if (bytes_to_write == 0)
        return;

    write_stream->write(working_buffer.begin(), static_cast<std::streamsize>(bytes_to_write));

    if (!write_stream->good())
        throwFromGCSStatus(write_stream->last_status(),
            fmt::format("while writing '{}' in bucket '{}'", key, bucket));
}

void WriteBufferFromGCS::finalizeImpl()
{
    /// Flush whatever remains in the working buffer, then close the upload.
    next();

    write_stream->Close();

    if (!write_stream->metadata())
        throwFromGCSStatus(write_stream->metadata().status(),
            fmt::format("while finalizing the upload of '{}' in bucket '{}'", key, bucket));
}

void WriteBufferFromGCS::cancelImpl() noexcept
{
    /// Abandon the (possibly resumable) upload without finalizing it. Dropping the stream leaves any
    /// resumable session unfinished; GCS garbage-collects incomplete uploads automatically.
    write_stream.reset();
}

}

#endif
