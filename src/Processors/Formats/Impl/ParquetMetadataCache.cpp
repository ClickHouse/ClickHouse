#include <Processors/Formats/Impl/ParquetMetadataCache.h>

#if USE_PARQUET
namespace DB
{
std::pair<String, String> extractFilePathAndETagFromReadBuffer(ReadBuffer & in)
{
    String full_path = getFileNameFromReadBuffer(in);
    String etag;
    /// Extract ETag from S3 metadata if available
    if (auto * s3_buffer = dynamic_cast<ReadBufferFromS3*>(&in))
    {
        try
        {
            ObjectMetadata metadata = s3_buffer->getObjectMetadataFromTheLastRequest();
            etag = metadata.etag;
        }
        catch (...)
        {
            /// If metadata not available, use a fallback
            etag = "s3_metadata_unavailable";
        }
    }
    return std::make_pair(full_path, etag);
}
}
#endif
