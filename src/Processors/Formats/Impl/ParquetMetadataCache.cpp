#include <Processors/Formats/Impl/ParquetMetadataCache.h>

#if USE_AWS_S3
#include <IO/ReadBufferFromS3.h>
#endif
#if USE_AZURE_BLOB_STORAGE
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#endif

#if USE_PARQUET
namespace DB
{

std::optional<ObjectMetadata> tryGetObjectMetadata(ReadBuffer & in)
{
    /// first try s3
    if (auto * s3_buffer = dynamic_cast<ReadBufferFromS3*>(&in))
    {
        try {
            ObjectMetadata metadata = s3_buffer->getObjectMetadataFromTheLastRequest();
            return metadata;
        }
        catch (...)
        {
            return std::nullopt;
        }
    }
    /// next try gcs/azure
    if (auto * azure_buffer = dynamic_cast<ReadBufferFromAzureBlobStorage*>(&in))
    {
        try {
            ObjectMetadata metadata = azure_buffer->getObjectMetadataFromTheLastRequest();
            return metadata;
        }
        catch (...)
        {
            return std::nullopt;
        }
    }
    return std::nullopt;
}
    
std::pair<String, String> extractObjectAttributes(ReadBuffer & in)
{
    auto log = getLogger("ParquetMetadataCache");
    /// Get the file name as the first part of the hash key
    String full_path = getFileNameFromReadBuffer(in);
    LOG_DEBUG(log, "got file path: {}", full_path);
    String etag;
    /// Get the object metadata
    if (auto maybe_metadata = tryGetObjectMetadata(in))
    {
        etag = maybe_metadata->etag;
        LOG_DEBUG(log, "got etag: {}", etag);
        return std::make_pair(full_path, etag);
    }
    LOG_DEBUG(log, "unable to get etag");
    return std::make_pair(full_path, "");
}
}
#endif
