#pragma once
#include "config.h"

#if USE_AWS_S3

#include <IO/ReadBufferFromIStream.h>
#include <aws/s3/model/GetObjectResult.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/Stopwatch.h>

namespace DB::S3
{
/// Wrapper for ReadBufferFromIStream to store GetObjectResult (session holder) with corresponding response body stream.
/// Tracks per-connection metrics: duration and bytes read.
class ReadBufferFromGetObjectResult : public ReadBufferFromIStream
{
    std::optional<Aws::S3::Model::GetObjectResult> result;
    ObjectMetadata metadata;

    Stopwatch watch;
    size_t bytes_read = 0;
    bool metrics_observed = false;

    void observeMetrics();
    bool nextImpl() override;

public:
    ReadBufferFromGetObjectResult(Aws::S3::Model::GetObjectResult && result_, size_t size_, Stopwatch && watch_);
    ~ReadBufferFromGetObjectResult() override;

    void releaseResult();

    bool isResultReleased() const { return !result; }

    ObjectMetadata getObjectMetadata() const { return metadata; }
};
}

#endif
