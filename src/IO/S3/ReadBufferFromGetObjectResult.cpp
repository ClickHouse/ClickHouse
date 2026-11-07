#include "config.h"

#if USE_AWS_S3

#include <IO/S3/ReadBufferFromGetObjectResult.h>
#include <Common/HistogramMetrics.h>

namespace HistogramMetrics
{
    extern Metric & S3ReadRequestDuration;
    extern Metric & S3ReadRequestBytes;
}

namespace DB::S3
{

ReadBufferFromGetObjectResult::ReadBufferFromGetObjectResult(Aws::S3::Model::GetObjectResult && result_, size_t size_, Stopwatch && watch_)
    : ReadBufferFromIStream(result_.GetBody(), size_), result(std::move(result_)), watch(std::move(watch_))
{
    metadata.size_bytes = result->GetContentLength();
    metadata.last_modified = Poco::Timestamp::fromEpochTime(result->GetLastModified().Seconds());
    metadata.etag = result->GetETag();
    metadata.attributes = result->GetMetadata();
}

ReadBufferFromGetObjectResult::~ReadBufferFromGetObjectResult()
{
    observeMetrics();
}

bool ReadBufferFromGetObjectResult::nextImpl()
{
    bool res = ReadBufferFromIStream::nextImpl();
    if (res)
        bytes_read += working_buffer.size();
    return res;
}

void ReadBufferFromGetObjectResult::releaseResult()
{
    observeMetrics();
    detachStream();
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
