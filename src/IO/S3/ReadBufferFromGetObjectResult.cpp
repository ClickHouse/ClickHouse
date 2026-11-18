#include "config.h"

#if USE_AWS_S3

#include <IO/S3/ReadBufferFromGetObjectResult.h>
#include <IO/StdStreamBufFromReadBuffer.h>
#include <Common/Exception.h>
#include <Common/HistogramMetrics.h>

namespace HistogramMetrics
{
    extern Metric & S3ReadRequestDuration;
    extern Metric & S3ReadRequestBytes;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace DB::S3
{

ReadBufferFromGetObjectResult::ReadBufferFromGetObjectResult(Aws::S3::Model::GetObjectResult && result_, size_t size_, Stopwatch && watch_)
    : BufferWithOwnMemory<ReadBuffer>(size_), result(std::move(result_)), watch(std::move(watch_))
{
    /// The S3 HTTP client hands the body of a response over as a ReadBuffer wrapped into a
    /// stream, because the AWS SDK accepts nothing else. Unwrap it and read from the buffer.
    auto * stream_buf = dynamic_cast<StdStreamBufFromReadBuffer *>(result->GetBody().rdbuf());
    if (!stream_buf)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The body of a GetObject response is not a ReadBuffer");
    body = &stream_buf->getReadBuffer();
    http_body = dynamic_cast<HTTPResponseReadBuffer *>(body);

    metadata.size_bytes = result->GetContentLength();
    metadata.last_modified = Poco::Timestamp::fromEpochTime(result->GetLastModified().Seconds());
    metadata.etag = result->GetETag();
    metadata.attributes = result->GetMetadata();

    /// An empty response has nothing to read at all.
    if (metadata.size_bytes == 0 && (!http_body || http_body->isResponseComplete()))
        stream_eof = true;
}

ReadBufferFromGetObjectResult::~ReadBufferFromGetObjectResult()
{
    observeMetrics();
}

bool ReadBufferFromGetObjectResult::nextImpl()
{
    if (!body)
        return false;

    /// Read into our own memory, or into the memory the caller set on us.
    if (body->supportsExternalBufferMode())
        body->set(internal_buffer.begin(), internal_buffer.size());

    if (!body->next())
    {
        stream_eof = true;
        return false;
    }

    working_buffer = body->buffer();
    bytes_read += working_buffer.size();

    /// Report the end of the response as early as it is known, so that the connection goes back
    /// to the pool without one more read: the framing of the response says so, or everything the
    /// `Content-Length` header announced has been read.
    if ((http_body && http_body->isResponseComplete())
        || (metadata.size_bytes > 0 && bytes_read >= static_cast<size_t>(metadata.size_bytes)))
        stream_eof = true;

    return true;
}

void ReadBufferFromGetObjectResult::releaseResult()
{
    observeMetrics();
    body = nullptr;
    result.reset();
}

void ReadBufferFromGetObjectResult::observeMetrics()
{
    if (metrics_observed)
        return;
    metrics_observed = true;

    HistogramMetrics::S3ReadRequestDuration.observe(static_cast<double>(watch.elapsedMicroseconds()));
    HistogramMetrics::S3ReadRequestBytes.observe(static_cast<double>(bytes_read));
}

}

#endif
