#pragma once
#include "config.h"

#if USE_AWS_S3

#include <IO/BufferWithOwnMemory.h>
#include <IO/HTTP/HTTPClientIO.h>
#include <IO/ReadBuffer.h>
#include <aws/s3/model/GetObjectResult.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/Stopwatch.h>

namespace DB::S3
{
/// Reads the body of a GetObject response, holding the result (and through it the HTTP session)
/// for as long as the body is being read.
///
/// The body itself is read by the buffer the S3 HTTP client put into the response, straight from
/// the socket into the memory of this buffer - or into the memory the caller passes with `set()`
/// in the external buffer mode.
/// Tracks per-connection metrics: duration and bytes read.
class ReadBufferFromGetObjectResult : public BufferWithOwnMemory<ReadBuffer>
{
    std::optional<Aws::S3::Model::GetObjectResult> result;
    /// The body of the response, owned by the stream inside `result`.
    ReadBuffer * body = nullptr;
    /// The same buffer, when it is one that knows the framing of the response and can tell that
    /// the body has been read to the end without one more read.
    HTTPResponseReadBuffer * http_body = nullptr;
    ObjectMetadata metadata;

    Stopwatch watch;
    size_t bytes_read = 0;
    bool stream_eof = false;
    bool metrics_observed = false;

    void observeMetrics();
    bool nextImpl() override;

public:
    ReadBufferFromGetObjectResult(Aws::S3::Model::GetObjectResult && result_, size_t size_, Stopwatch && watch_);
    ~ReadBufferFromGetObjectResult() override;

    bool supportsExternalBufferMode() const override { return true; }

    void releaseResult();

    bool isResultReleased() const { return !result; }

    /// Whether everything the response contains has been read. Reported as soon as the last byte
    /// is handed out, so that the connection can go back to the pool without one more read.
    bool isStreamEof() const { return stream_eof; }

    ObjectMetadata getObjectMetadata() const { return metadata; }
};
}

#endif
