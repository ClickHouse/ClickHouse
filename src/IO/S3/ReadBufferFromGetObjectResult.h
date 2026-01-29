#pragma once
#include "config.h"

#if USE_AWS_S3

#include <IO/ReadBufferFromIStream.h>
#include <aws/s3/model/GetObjectResult.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB::S3
{
/// Wrapper for ReadBufferFromIStream to store GetObjectResult (session holder) with corresponding response body stream
class ReadBufferFromGetObjectResult : public ReadBufferFromIStream
{
    std::optional<Aws::S3::Model::GetObjectResult> result;
    ObjectMetadata metadata;

public:
    ReadBufferFromGetObjectResult(Aws::S3::Model::GetObjectResult && result_, size_t size_)
        : ReadBufferFromIStream(result_.GetBody(), size_), result(std::move(result_))
    {
        metadata.size_bytes = result->GetContentLength();
        metadata.last_modified = Poco::Timestamp::fromEpochTime(result->GetLastModified().Seconds());
        metadata.etag = result->GetETag();
        metadata.attributes = result->GetMetadata();
    }

    /// Allows to safely release the result and detach the underlying body stream from the buffer.
    /// The buffer can still be used, but subsequent reads won't return any more data.
    void releaseResult()
    {
        detachStream();
        result.reset();
    }

    bool isResultReleased() const { return !result; }

    ObjectMetadata getObjectMetadata() const { return metadata; }
};
}

#endif
