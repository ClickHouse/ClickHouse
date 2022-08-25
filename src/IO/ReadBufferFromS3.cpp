#include <Common/config.h>
#include "IO/S3Common.h"

#if USE_AWS_S3

#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadBufferFromS3.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

#include <Common/Stopwatch.h>
#include <Common/Throttler.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>

#include <utility>


namespace ProfileEvents
{
    extern const Event ReadBufferFromS3Microseconds;
    extern const Event ReadBufferFromS3Bytes;
    extern const Event ReadBufferFromS3RequestsErrors;
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
    std::shared_ptr<const Aws::S3::S3Client> client_ptr_,
    const String & bucket_,
    const String & key_,
    const String & version_id_,
    UInt64 max_single_read_retries_,
    const ReadSettings & settings_,
    bool use_external_buffer_,
    size_t offset_,
    size_t read_until_position_,
    bool restricted_seek_)
    : ReadBufferFromFileBase(settings_.remote_fs_buffer_size, nullptr, 0)
    , client_ptr(std::move(client_ptr_))
    , bucket(bucket_)
    , key(key_)
    , version_id(version_id_)
    , max_single_read_retries(max_single_read_retries_)
    , offset(offset_)
    , read_until_position(read_until_position_)
    , read_settings(settings_)
    , use_external_buffer(use_external_buffer_)
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
                else
                {
                    /// use the buffer returned by `impl`
                    BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());
                }
            }

            /// Try to read a next portion of data.
            next_result = impl->next();
            watch.stop();
            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3Microseconds, watch.elapsedMicroseconds());
            break;
        }
        catch (const Exception & e)
        {
            watch.stop();
            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3Microseconds, watch.elapsedMicroseconds());
            ProfileEvents::increment(ProfileEvents::ReadBufferFromS3RequestsErrors, 1);

            LOG_DEBUG(
                log,
                "Caught exception while reading S3 object. Bucket: {}, Key: {}, Version: {}, Offset: {}, Attempt: {}, Message: {}",
                bucket,
                key,
                version_id.empty() ? "Latest" : version_id,
                getPosition(),
                attempt,
                e.message());

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

    BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());

    ProfileEvents::increment(ProfileEvents::ReadBufferFromS3Bytes, working_buffer.size());
    offset += working_buffer.size();
    if (read_settings.remote_throttler)
        read_settings.remote_throttler->add(working_buffer.size());

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
            && static_cast<size_t>(offset_) >= offset - working_buffer.size()
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

size_t ReadBufferFromS3::getFileSize()
{
    if (file_size)
        return *file_size;

    auto object_size = S3::getObjectSize(client_ptr, bucket, key, version_id);

    file_size = object_size;
    return *file_size;
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

SeekableReadBuffer::Range ReadBufferFromS3::getRemainingReadRange() const
{
    return Range{ .left = static_cast<size_t>(offset), .right = read_until_position ? std::optional{read_until_position - 1} : std::nullopt };
}

std::unique_ptr<ReadBuffer> ReadBufferFromS3::initialize()
{
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    if (!version_id.empty())
    {
        req.SetVersionId(version_id);
    }

    /**
     * If remote_filesystem_read_method = 'threadpool', then for MergeTree family tables
     * exact byte ranges to read are always passed here.
     */
    if (read_until_position)
    {
        if (offset >= read_until_position)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);

        req.SetRange(fmt::format("bytes={}-{}", offset, read_until_position - 1));
        LOG_TEST(
            log,
            "Read S3 object. Bucket: {}, Key: {}, Version: {}, Range: {}-{}",
            bucket,
            key,
            version_id.empty() ? "Latest" : version_id,
            offset,
            read_until_position - 1);
    }
    else
    {
        if (offset)
            req.SetRange(fmt::format("bytes={}-", offset));
        LOG_TEST(
            log,
            "Read S3 object. Bucket: {}, Key: {}, Version: {}, Offset: {}",
            bucket,
            key,
            version_id.empty() ? "Latest" : version_id,
            offset);
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

SeekableReadBufferPtr ReadBufferS3Factory::getReader()
{
    const auto next_range = range_generator.nextRange();
    if (!next_range)
    {
        return nullptr;
    }

    auto reader = std::make_shared<ReadBufferFromS3>(
        client_ptr,
        bucket,
        key,
        version_id,
        s3_max_single_read_retries,
        read_settings,
        false /*use_external_buffer*/,
        next_range->first,
        next_range->second);
    return reader;
}

off_t ReadBufferS3Factory::seek(off_t off, [[maybe_unused]] int whence)
{
    range_generator = RangeGenerator{object_size, range_step, static_cast<size_t>(off)};
    return off;
}

size_t ReadBufferS3Factory::getFileSize()
{
    return object_size;
}
}

#endif
