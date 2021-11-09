#include <Common/config.h>

#if USE_AWS_S3

#include <Disks/DiskCache.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadSettings.h>
#include <Common/Stopwatch.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <base/logger_useful.h>
#include <base/sleep.h>

#include <utility>
#include <regex>


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
    extern const int LOGICAL_ERROR;
}


ReadBufferFromS3::ReadBufferFromS3(
    std::shared_ptr<Aws::S3::S3Client> client_ptr_, std::shared_ptr<DiskCache> disk_cache_, const String & bucket_, const String & key_,
    UInt64 max_single_read_retries_, const ReadSettings & settings_, bool use_external_buffer_, size_t read_until_position_)
    : SeekableReadBuffer(nullptr, 0)
    , client_ptr(std::move(client_ptr_))
    , disk_cache(std::move(disk_cache_))
    , bucket(bucket_)
    , key(key_)
    , max_single_read_retries(max_single_read_retries_)
    , read_settings(settings_)
    , use_external_buffer(use_external_buffer_)
    , read_until_position(read_until_position_)
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

    if (!disk_cache)
        ProfileEvents::increment(ProfileEvents::S3ReadBytes, working_buffer.size());

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

struct Range
{
    size_t start = 0;
    size_t end = 0;
    size_t size = 0;

    bool parse(const String & content_range)
    { // TODO: do not use regex here
        // supported   '<unit> <start>-<end>/<size>'
        // unsuppotred '<unit> <start>-<end>/*'
        // unsuppurted '<unit> */<size>'
        // unsupported unit except 'bytes'
        std::regex pattern("bytes\\W+(\\d+)-(\\d+)/(\\d+)");
        std::smatch match;
        if (!std::regex_match(content_range, match, pattern) || match.size() != 4)
            return false;

        start = strtoull(match[1]);
        end = strtoull(match[2]);
        size = strtoull(match[3]);

        return (end >= start && size > end);
    }

    size_t strtoull(const String & str)
    {
        char * strend;
        return std::strtoull(str.data(), &strend, 10);
    }
};

class DiskCacheDownloaderS3 : public DiskCacheDownloader
{
public:
    DiskCacheDownloaderS3(std::shared_ptr<Aws::S3::S3Client> client_ptr_, const String & bucket_,
        const String & key_, Aws::S3::Model::GetObjectResult & read_result_, size_t buffer_size_) :
        client_ptr(client_ptr_), bucket(bucket_), key(key_), read_result(read_result_), buffer_size(buffer_size_)
    {
        log = &Poco::Logger::get("DiskCacheDownloaderS3");
    }

    ~DiskCacheDownloaderS3() override {}

    RemoteFSStream get(size_t offset, size_t size) override
    {
        Aws::S3::Model::GetObjectRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);

        /**
         * If remote_filesystem_read_method = 'read_threadpool', then for MergeTree family tables
         * exact byte ranges to read are always passed here.
         */
        if (size)
        {
            req.SetRange(fmt::format("bytes={}-{}", offset, offset + size - 1));
            LOG_DEBUG(log, "Read S3 object. Bucket: {}, Key: {}, Range: {}-{}", bucket, key, offset, offset + size - 1);
        }
        else
        {
            req.SetRange(fmt::format("bytes={}-", offset));
            LOG_DEBUG(log, "Read S3 object. Bucket: {}, Key: {}, Offset: {}", bucket, key, offset);
        }

        Aws::S3::Model::GetObjectOutcome outcome = client_ptr->GetObject(req);

        RemoteFSStream res;

        if (!outcome.IsSuccess())
        {
            if (outcome.GetError().GetExceptionName() == "InvalidRange")
            {   /// offset is out of available size
                /// May be when offset is equal to file size
                return res;
            }
            else
                throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
        }

        read_result = outcome.GetResultWithOwnership();

        String range_header = read_result.GetContentRange();
        Range range;
        if (!range.parse(range_header))
        {
            LOG_ERROR(log, "Can't parse range: {}", range_header);
        }
        else
        {
            res.expected_size = range.end - range.start + 1;
            res.file_size = range.size;
        }

        res.stream = std::make_unique<ReadBufferFromIStream>(read_result.GetBody(), buffer_size);
        return res;
    }

private:
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    String bucket;
    String key;
    Aws::S3::Model::GetObjectResult & read_result;
    size_t buffer_size;
    Poco::Logger * log;
};

std::unique_ptr<ReadBuffer> ReadBufferFromS3::initialize()
{
    if (read_until_position)
    {
        if (offset >= read_until_position)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);
    }

    auto downloader = std::make_shared<DiskCacheDownloaderS3>(client_ptr, bucket, key, read_result, read_settings.remote_fs_buffer_size);

    if (disk_cache)
    {
        String cache_key = bucket + "/" + key;
        return disk_cache->find(cache_key, offset, read_until_position ? read_until_position - offset : 0, downloader);
    }

    return downloader->get(offset, read_until_position ? read_until_position - offset : 0).stream;
}

}

#endif
