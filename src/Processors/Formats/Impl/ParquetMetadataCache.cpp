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
/*
I want to try to cast the ReadBuffer to either an s3 or gcs/azure readbuffer
(either a ReadBufferFromS3 or ReadBufferFromAzureBlobStorage).
Depending on which I get, I will try to get the object metadata from the last request.
If I can't get the metadata, I will return std::nullopt.
*/
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
/*
I want to first get the file name as the first part of the hash key
Next I want to get the file size as an optional part of the hash key
in the event that we don't have the etag (entity tag) attribute from
remote object storage
Lastly I will try to get the etag attribute if possible and use it
as the final part of the hash key
*/
{
    /// Get the file name as the first part of the hash key
    String full_path = getFileNameFromReadBuffer(in);
    String file_size;
    String etag;
    /// Get the file size as an optional part of the hash key
    if (auto maybe_size = tryGetFileSizeFromReadBuffer(in))
    {
        file_size = std::to_string(*maybe_size);
    }
    /// Get the object metadata
    if (auto maybe_metadata = tryGetObjectMetadata(in))
    {
        etag = maybe_metadata->etag;
        return std::make_pair(full_path, etag);
    }
    return std::make_pair(full_path, file_size);
}
}
#endif
