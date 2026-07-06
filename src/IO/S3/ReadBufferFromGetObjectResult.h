#pragma once
#include "config.h"

#if USE_AWS_S3

#include <IO/ReadBufferFromIStream.h>
#include <aws/s3/model/GetObjectResult.h>

namespace DB::S3
{
/// Wrapper for ReadBufferFromIStream to store GetObjectResult (session holder) with corresponding response body stream
class ReadBufferFromGetObjectResult : public ReadBufferFromIStream
{
    std::optional<Aws::S3::Model::GetObjectResult> result;

public:
    ReadBufferFromGetObjectResult(Aws::S3::Model::GetObjectResult && result_, size_t size_)
        : ReadBufferFromIStream(result_.GetBody(), size_), result(std::move(result_))
    {
    }

    /// Allows to safely release the result and detach the underlying body stream from the buffer.
    /// The buffer can still be used, but subsequent reads won't return any more data.
    void releaseResult()
    {
        detachStream();
        result.reset();
    }

    bool isResultReleased() const { return !result; }
};
}

#endif
