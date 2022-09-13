#include <Common/config.h>

#if USE_AWS_S3

#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadBufferFromS3.h>
#include <Common/Stopwatch.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

#include <base/logger_useful.h>
#include <base/sleep.h>

#include <utility>


namespace ProfileEvents
{
    extern const Event S3ReadMicroseconds;
    extern const Event S3ReadBytes;
    extern const Event S3ReadRequestsErrors;
    extern const Event ReadBufferSeekCancelConnection;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}


ReadBufferFromS3::ReadBufferFromS3(
    std::shared_ptr<Aws::S3::S3Client> client_ptr_,
    const String & bucket_,
    const String & key_,
    UInt64 max_single_read_retries_,
    const ReadSettings & settings_,
    bool use_external_buffer_,
    size_t read_until_position_,
    bool restricted_seek_)
    : SeekableReadBufferWithSize(nullptr, 0)
    , client_ptr(std::move(client_ptr_))
    , bucket(bucket_)
    , key(key_)
    , max_single_read_retries(max_single_read_retries_)
    , read_settings(settings_)
    , use_external_buffer(use_external_buffer_)
    , read_until_position(read_until_position_)
    , restricted_seek(restricted_seek_)
{
}

bool ReadBufferFromS3::nextImpl()
{
    if (read_until_position)
    {
        if (read_until_position == offset)
            return false;

        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);
    }

    bool next_result = false;

    if (impl)
    {
        if (use_external_buffer)
        {
            /**
            * use_external_buffer -- means we read into the buffer which
            * was passed to us from somewhere else. We do not check whether
            * previously returned buffer was read or not (no hasPendingData() check is needed),
            * because this branch means we are prefetching data,
            * each nextImpl() call we can fill a different buffer.
            */
            impl->set(internal_buffer.begin(), internal_buffer.size());
            assert(working_buffer.begin() != nullptr);
            assert(!internal_buffer.empty());
        }
        else
        {
            /**
            * impl was initialized before, pass position() to it to make
            * sure there is no pending data which was not read.
            */
            impl->position() = position();
            assert(!impl->hasPendingData());
        }
    }

    size_t sleep_time_with_backoff_milliseconds = 100;
    for (size_t attempt = 0; (attempt < max_single_read_retries) && !next_result; ++attempt)
    {
        Stopwatch watch;
        try
        {
            if (!impl)
            {
                impl = initialize();

                if (use_external_buffer)
                {
                    impl->set(internal_buffer.begin(), internal_buffer.size());
                    assert(working_buffer.begin() != nullptr);
                    assert(!internal_buffer.empty());
                }
            }

            /// Try to read a next portion of data.
            next_result = impl->next();
            watch.stop();
            ProfileEvents::increment(ProfileEvents::S3ReadMicroseconds, watch.elapsedMicroseconds());
            break;
        }
        catch (const Exception & e)
        {
            watch.stop();
            ProfileEvents::increment(ProfileEvents::S3ReadMicroseconds, watch.elapsedMicroseconds());
            ProfileEvents::increment(ProfileEvents::S3ReadRequestsErrors, 1);

            LOG_DEBUG(log, "Caught exception while reading S3 object. Bucket: {}, Key: {}, Offset: {}, Attempt: {}, Message: {}",
                    bucket, key, getPosition(), attempt, e.message());

            if (attempt + 1 == max_single_read_retries)
                throw;

            /// Pause before next attempt.
            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;

            /// Try to reinitialize `impl`.
            impl.reset();
        }
    }

    if (!next_result)
        return false;

    BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset()); /// use the buffer returned by `impl`

    ProfileEvents::increment(ProfileEvents::S3ReadBytes, working_buffer.size());
    offset += working_buffer.size();

    return true;
}


off_t ReadBufferFromS3::seek(off_t offset_, int whence)
{
    if (offset_ == offset && whence == SEEK_SET)
        return offset;

    if (impl && restricted_seek)
        throw Exception(
            ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
            "Seek is allowed only before first read attempt from the buffer (current offset: {}, new offset: {}, reading until position: {}, available: {})",
            offset, offset_, read_until_position, available());

    if (whence != SEEK_SET)
        throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (offset_ < 0)
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset_), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    if (!restricted_seek)
    {
        if (!working_buffer.empty()
            && size_t(offset_) >= offset - working_buffer.size()
            && offset_ < offset)
        {
            pos = working_buffer.end() - (offset - offset_);
            assert(pos >= working_buffer.begin());
            assert(pos <= working_buffer.end());

            return getPosition();
        }

        auto position = getPosition();
        if (offset_ > position)
        {
            size_t diff = offset_ - position;
            if (diff < read_settings.remote_read_min_bytes_for_seek)
            {
                ignore(diff);
                return offset_;
            }
        }

        resetWorkingBuffer();
        if (impl)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferSeekCancelConnection);
            impl.reset();
        }
    }

    offset = offset_;
    return offset;
}

std::optional<size_t> ReadBufferFromS3::getTotalSize()
{
    if (file_size)
        return file_size;

    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);

    auto outcome = client_ptr->HeadObject(request);
    auto head_result = outcome.GetResultWithOwnership();
    file_size = head_result.GetContentLength();
    return file_size;
}

off_t ReadBufferFromS3::getPosition()
{
    return offset - available();
}

void ReadBufferFromS3::setReadUntilPosition(size_t position)
{
    if (position != static_cast<size_t>(read_until_position))
    {
        read_until_position = position;
        impl.reset();
    }
}

std::unique_ptr<ReadBuffer> ReadBufferFromS3::initialize()
{
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);

    /**
     * If remote_filesystem_read_method = 'threadpool', then for MergeTree family tables
     * exact byte ranges to read are always passed here.
     */
    if (read_until_position)
    {
        if (offset >= read_until_position)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);

        req.SetRange(fmt::format("bytes={}-{}", offset, read_until_position - 1));
        LOG_TEST(log, "Read S3 object. Bucket: {}, Key: {}, Range: {}-{}", bucket, key, offset, read_until_position - 1);
    }
    else
    {
        if (offset)
            req.SetRange(fmt::format("bytes={}-", offset));
        LOG_TEST(log, "Read S3 object. Bucket: {}, Key: {}, Offset: {}", bucket, key, offset);
    }

    Aws::S3::Model::GetObjectOutcome outcome = client_ptr->GetObject(req);

    if (outcome.IsSuccess())
    {
        read_result = outcome.GetResultWithOwnership();

        size_t buffer_size = use_external_buffer ? 0 : read_settings.remote_fs_buffer_size;
        return std::make_unique<ReadBufferFromIStream>(read_result.GetBody(), buffer_size);
    }
    else
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
}

}

#endif
