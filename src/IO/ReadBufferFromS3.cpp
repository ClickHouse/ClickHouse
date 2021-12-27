#include <Common/config.h>

#if USE_AWS_S3

#    include <IO/ReadBufferFromIStream.h>
#    include <IO/ReadBufferFromS3.h>
#    include <Common/Stopwatch.h>

#    include <aws/s3/S3Client.h>
#    include <aws/s3/model/GetObjectRequest.h>
#    include <common/logger_useful.h>

#    include <utility>


namespace ProfileEvents
{
    extern const Event S3ReadMicroseconds;
    extern const Event S3ReadBytes;
    extern const Event S3ReadRequestsErrors;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


ReadBufferFromS3::ReadBufferFromS3(
    std::shared_ptr<Aws::S3::S3Client> client_ptr_, const String & bucket_, const String & key_, UInt64 max_single_read_retries_, size_t buffer_size_)
    : SeekableReadBuffer(nullptr, 0)
    , client_ptr(std::move(client_ptr_))
    , bucket(bucket_)
    , key(key_)
    , max_single_read_retries(max_single_read_retries_)
    , buffer_size(buffer_size_)
{
}

bool ReadBufferFromS3::nextImpl()
{
    Stopwatch watch;
    bool next_result = false;
    auto sleep_time_with_backoff_milliseconds = std::chrono::milliseconds(100);

    if (!impl)
        impl = initialize();

    for (size_t attempt = 0; attempt < max_single_read_retries; ++attempt)
    {
        try
        {
            next_result = impl->next();
            /// FIXME. 1. Poco `istream` cannot read less than buffer_size or this state is being discarded during
            ///           istream <-> iostream conversion. `gcount` always contains 0,
            ///           that's why we always have error "Cannot read from istream at offset 0".

            break;
        }
        catch (const Exception & e)
        {
            ProfileEvents::increment(ProfileEvents::S3ReadRequestsErrors, 1);

            LOG_INFO(log, "Caught exception while reading S3 object. Bucket: {}, Key: {}, Offset: {}, Attempt: {}, Message: {}",
                    bucket, key, getPosition(), attempt, e.message());

            impl.reset();
            impl = initialize();
        }

        std::this_thread::sleep_for(sleep_time_with_backoff_milliseconds);
        sleep_time_with_backoff_milliseconds *= 2;
    }

    watch.stop();
    ProfileEvents::increment(ProfileEvents::S3ReadMicroseconds, watch.elapsedMicroseconds());
    if (!next_result)
        return false;

    working_buffer = internal_buffer = impl->buffer();
    pos = working_buffer.begin();

    ProfileEvents::increment(ProfileEvents::S3ReadBytes, internal_buffer.size());

    offset += working_buffer.size();

    return true;
}

off_t ReadBufferFromS3::seek(off_t offset_, int whence)
{
    if (impl)
        throw Exception("Seek is allowed only before first read attempt from the buffer.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (whence != SEEK_SET)
        throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (offset_ < 0)
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset_), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    offset = offset_;

    return offset;
}

off_t ReadBufferFromS3::getPosition()
{
    return offset - available();
}

std::unique_ptr<ReadBuffer> ReadBufferFromS3::initialize()
{
    LOG_TRACE(log, "Read S3 object. Bucket: {}, Key: {}, Offset: {}", bucket, key, offset);

    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);

    if (offset)
        req.SetRange(fmt::format("bytes={}-", offset));

    Aws::S3::Model::GetObjectOutcome outcome = client_ptr->GetObject(req);

    if (outcome.IsSuccess())
    {
        read_result = outcome.GetResultWithOwnership();
        return std::make_unique<ReadBufferFromIStream>(read_result.GetBody(), buffer_size);
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}

}

#endif
