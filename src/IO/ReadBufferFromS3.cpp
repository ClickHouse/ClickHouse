#include <IO/HTTPCommon.h>
#include <IO/S3Common.h>
#include "config.h"

#if USE_AWS_S3

#include <IO/ReadBufferFromS3.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/S3/Requests.h>

#include <Common/Stopwatch.h>
#include <Common/Throttler.h>
#include <Common/logger_useful.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <base/sleep.h>

#include <utility>


namespace ProfileEvents
{
    extern const Event ReadBufferFromS3Microseconds;
    extern const Event ReadBufferFromS3InitMicroseconds;
    extern const Event ReadBufferFromS3Bytes;
    extern const Event ReadBufferFromS3RequestsErrors;
    extern const Event ReadBufferSeekCancelConnection;
    extern const Event S3GetObject;
    extern const Event DiskS3GetObject;
}

namespace DB
{

namespace S3RequestSetting
{
    extern const S3RequestSettingsUInt64 max_single_read_retries;
}

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_ALLOCATE_MEMORY;
}


ReadBufferFromS3::ReadBufferFromS3(
    std::shared_ptr<const S3::Client> client_ptr_,
    const String & bucket_,
    const String & key_,
    const String & version_id_,
    const S3::S3RequestSettings & request_settings_,
    const ReadSettings & settings_,
    bool use_external_buffer_,
    size_t offset_,
    size_t read_until_position_,
    bool restricted_seek_,
    std::optional<size_t> file_size_)
    : ReadBufferFromFileBase()
    , client_ptr(std::move(client_ptr_))
    , bucket(bucket_)
    , key(key_)
    , version_id(version_id_)
    , request_settings(request_settings_)
    , offset(offset_)
    , read_until_position(read_until_position_)
    , read_settings(settings_)
    , use_external_buffer(use_external_buffer_)
    , restricted_seek(restricted_seek_)
{
    file_size = file_size_;
}

bool ReadBufferFromS3::nextImpl()
{
    if (read_until_position)
    {
        if (read_until_position == offset)
            return false;

        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset.load(), read_until_position - 1);
    }

    if (impl)
    {
        if (impl->isResultReleased())
            return false;

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

    bool next_result = false;
    size_t sleep_time_with_backoff_milliseconds = 100;
    for (size_t attempt = 1; !next_result; ++attempt)
    {
        bool last_attempt = attempt >= request_settings[S3RequestSetting::max_single_read_retries];

        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadBufferFromS3Microseconds);

        try
        {
            if (!impl)
            {
                impl = initialize(attempt);

                if (use_external_buffer)
                {
                    impl->set(internal_buffer.begin(), internal_buffer.size());
                    assert(working_buffer.begin() != nullptr);
                    assert(!internal_buffer.empty());
                }
                else
                {
                    /// use the buffer returned by `impl`
                    BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());
                }
            }

            /// Try to read a next portion of data.
            next_result = impl->next();
            break;
        }
        catch (...)
        {
            if (!processException(getPosition(), attempt) || last_attempt)
                throw;

            /// Pause before next attempt.
            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;

            /// Try to reinitialize `impl`.
            resetWorkingBuffer();
            impl.reset();
        }
    }

    if (!next_result)
    {
        read_all_range_successfully = true;
        // release result to free pooled HTTP session for reuse
        impl->releaseResult();
        return false;
    }

    BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());

    ProfileEvents::increment(ProfileEvents::ReadBufferFromS3Bytes, working_buffer.size());
    offset += working_buffer.size();

    // release result if possible to free pooled HTTP session for better reuse
    bool is_read_until_position = read_until_position && read_until_position == offset;
    if (impl->isStreamEof() || is_read_until_position)
        impl->releaseResult();

    if (read_settings.remote_throttler)
        read_settings.remote_throttler->add(working_buffer.size());

    return true;
}


size_t ReadBufferFromS3::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const
{
    size_t initial_n = n;
    size_t sleep_time_with_backoff_milliseconds = 100;
    for (size_t attempt = 1; n > 0; ++attempt)
    {
        bool last_attempt = attempt >= request_settings[S3RequestSetting::max_single_read_retries];
        size_t bytes_copied = 0;

        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadBufferFromS3Microseconds);

        std::optional<Aws::S3::Model::GetObjectResult> result;

        try
        {
            result = sendRequest(attempt, range_begin, range_begin + n - 1);
            std::istream & istr = result->GetBody();

            bool cancelled = false;
            copyFromIStreamWithProgressCallback(istr, to, n, progress_callback, &bytes_copied, &cancelled);

            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3Bytes, bytes_copied);

            if (cancelled)
                return initial_n - n + bytes_copied;

            if (read_settings.remote_throttler)
                read_settings.remote_throttler->add(bytes_copied);

            /// Read remaining bytes after the end of the payload
            istr.ignore(INT64_MAX);
        }
        catch (...)
        {
            if (!processException(range_begin, attempt) || last_attempt)
                throw;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }

        range_begin += bytes_copied;
        to += bytes_copied;
        n -= bytes_copied;
    }

    return initial_n;
}

bool ReadBufferFromS3::processException(size_t read_offset, size_t attempt) const
{
    ProfileEvents::increment(ProfileEvents::ReadBufferFromS3RequestsErrors, 1);

    LOG_DEBUG(
        log,
        "Caught exception while reading S3 object. Bucket: {}, Key: {}, Version: {}, Offset: {}, "
        "Attempt: {}/{}, Message: {}",
        bucket, key, version_id.empty() ? "Latest" : version_id, read_offset, attempt, request_settings[S3RequestSetting::max_single_read_retries].value,
        getCurrentExceptionMessage(/* with_stacktrace = */ false));


    if (auto * s3_exception = current_exception_cast<S3Exception *>())
    {
        /// It doesn't make sense to retry Access Denied or No Such Key
        if (!s3_exception->isRetryableError())
        {
            s3_exception->addMessage("while reading key: {}, from bucket: {}", key, bucket);
            return false;
        }
    }

    /// It doesn't make sense to retry allocator errors
    if (getCurrentExceptionCode() == ErrorCodes::CANNOT_ALLOCATE_MEMORY)
    {
        tryLogCurrentException(log);
        return false;
    }

    return true;
}


off_t ReadBufferFromS3::seek(off_t offset_, int whence)
{
    if (offset_ == getPosition() && whence == SEEK_SET)
        return offset_;

    read_all_range_successfully = false;

    if (impl && restricted_seek)
    {
        throw Exception(
            ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
            "Seek is allowed only before first read attempt from the buffer (current offset: "
            "{}, new offset: {}, reading until position: {}, available: {})",
            getPosition(), offset_, read_until_position.load(), available());
    }

    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset_);

    if (!restricted_seek)
    {
        if (!working_buffer.empty()
            && static_cast<size_t>(offset_) >= offset - working_buffer.size()
            && offset_ < offset)
        {
            pos = working_buffer.end() - (offset - offset_);
            assert(pos >= working_buffer.begin());
            assert(pos < working_buffer.end());

            return getPosition();
        }

        off_t position = getPosition();
        if (impl && offset_ > position)
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
            if (!atEndOfRequestedRangeGuess())
                ProfileEvents::increment(ProfileEvents::ReadBufferSeekCancelConnection);
            impl.reset();
        }
    }

    offset = offset_;
    return offset;
}

std::optional<size_t> ReadBufferFromS3::tryGetFileSize()
{
    if (file_size)
        return file_size;

    auto object_size = S3::getObjectSize(*client_ptr, bucket, key, version_id);

    file_size = object_size;
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
        read_all_range_successfully = false;

        if (impl)
        {
            if (!atEndOfRequestedRangeGuess())
                ProfileEvents::increment(ProfileEvents::ReadBufferSeekCancelConnection);
            offset = getPosition();
            resetWorkingBuffer();
            impl.reset();
        }
        read_until_position = position;
    }
}

void ReadBufferFromS3::setReadUntilEnd()
{
    if (read_until_position)
    {
        read_all_range_successfully = false;

        read_until_position = 0;
        if (impl)
        {
            if (!atEndOfRequestedRangeGuess())
                ProfileEvents::increment(ProfileEvents::ReadBufferSeekCancelConnection);
            offset = getPosition();
            resetWorkingBuffer();
            impl.reset();
        }
    }
}

bool ReadBufferFromS3::atEndOfRequestedRangeGuess()
{
    if (!impl)
        return true;
    if (read_until_position)
        return getPosition() >= read_until_position;
    if (file_size)
        return getPosition() >= static_cast<off_t>(*file_size);
    return false;
}

std::unique_ptr<S3::ReadBufferFromGetObjectResult> ReadBufferFromS3::initialize(size_t attempt)
{
    read_all_range_successfully = false;

    /**
     * If remote_filesystem_read_method = 'threadpool', then for MergeTree family tables
     * exact byte ranges to read are always passed here.
     */
    if (read_until_position && offset >= read_until_position)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset.load(), read_until_position - 1);

    auto read_result = sendRequest(attempt, offset, read_until_position ? std::make_optional(read_until_position - 1) : std::nullopt);

    size_t buffer_size = use_external_buffer ? 0 : read_settings.remote_fs_buffer_size;
    return std::make_unique<S3::ReadBufferFromGetObjectResult>(std::move(read_result), buffer_size);
}

Aws::S3::Model::GetObjectResult ReadBufferFromS3::sendRequest(size_t attempt, size_t range_begin, std::optional<size_t> range_end_incl) const
{
    S3::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    if (!version_id.empty())
        req.SetVersionId(version_id);

    req.SetAdditionalCustomHeaderValue("clickhouse-request", fmt::format("attempt={}", attempt));

    if (range_end_incl)
    {
        req.SetRange(fmt::format("bytes={}-{}", range_begin, *range_end_incl));
        LOG_TEST(
            log, "Read S3 object. Bucket: {}, Key: {}, Version: {}, Range: {}-{}",
            bucket, key, version_id.empty() ? "Latest" : version_id, range_begin, *range_end_incl);
    }
    else if (range_begin)
    {
        req.SetRange(fmt::format("bytes={}-", range_begin));
        LOG_TEST(
            log, "Read S3 object. Bucket: {}, Key: {}, Version: {}, Offset: {}",
            bucket, key, version_id.empty() ? "Latest" : version_id, range_begin);
    }

    ProfileEvents::increment(ProfileEvents::S3GetObject);
    if (client_ptr->isClientForDisk())
        ProfileEvents::increment(ProfileEvents::DiskS3GetObject);

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadBufferFromS3InitMicroseconds);

    // We do not know in advance how many bytes we are going to consume, to avoid blocking estimated it from below
    CurrentThread::IOScope io_scope(read_settings.io_scheduling);
    Aws::S3::Model::GetObjectOutcome outcome = client_ptr->GetObject(req);

    if (outcome.IsSuccess())
        return outcome.GetResultWithOwnership();

    const auto & error = outcome.GetError();
    throw S3Exception(error.GetMessage(), error.GetErrorType());
}

}

#endif
