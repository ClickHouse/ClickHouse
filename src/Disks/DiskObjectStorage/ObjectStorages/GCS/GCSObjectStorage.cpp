#include <Disks/DiskObjectStorage/ObjectStorages/GCS/GCSObjectStorage.h>

#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/BlobStorageLogWriter.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Throttler.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <future>
#include <mutex>

#if USE_AWS_S3
#    include <Common/threadPoolCallbackRunner.h>
#    include <Core/Settings.h>
#    include <IO/WriteBufferFromS3.h>
#endif

#if USE_GOOGLE_CLOUD
#    include <absl/strings/cord.h>
#endif

namespace ProfileEvents
{
extern const Event GCSGetRequestThrottlerCount;
extern const Event GCSGetRequestThrottlerSleepMicroseconds;
extern const Event GCSPutRequestThrottlerCount;
extern const Event GCSPutRequestThrottlerSleepMicroseconds;
extern const Event ReadBufferFromGCSMicroseconds;
extern const Event ReadBufferFromGCSInitMicroseconds;
extern const Event ReadBufferFromGCSBytes;
extern const Event ReadBufferFromGCSRequestsErrors;
extern const Event WriteBufferFromGCSMicroseconds;
extern const Event WriteBufferFromGCSBytes;
extern const Event WriteBufferFromGCSRequestsErrors;
}

namespace DB
{

namespace Setting
{
#if USE_AWS_S3
extern const SettingsBool s3_validate_request_settings;
#endif
}

namespace S3RequestSetting
{
#if USE_AWS_S3
extern const S3RequestSettingsUInt64 strict_upload_part_size;
extern const S3RequestSettingsUInt64 min_upload_part_size;
extern const S3RequestSettingsUInt64 max_single_part_upload_size;
#endif
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int FILE_DOESNT_EXIST;
extern const int NOT_IMPLEMENTED;
extern const int S3_ERROR;
}

static String expandConfigString(const Poco::Util::AbstractConfiguration & config, const String & key, const ContextPtr & context)
{
    return context->getMacros()->expand(config.getString(key));
}

static String normalizeKeyPrefix(String key_prefix)
{
    if (key_prefix.empty())
        return key_prefix;

    if (key_prefix.starts_with('/'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS key prefix must be relative, got '{}'", key_prefix);

    if (!key_prefix.ends_with('/'))
        key_prefix.push_back('/');

    return key_prefix;
}


#if USE_GOOGLE_CLOUD
namespace
{

const String & objectName(const String & path)
{
    if (path.starts_with('/'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS object name must be relative, got '{}'", path);
    return path;
}

ObjectMetadata metadataFromCloud(const google::cloud::storage::ObjectMetadata & object, bool with_tags)
{
    ObjectMetadata metadata;
    metadata.size_bytes = object.size();
    metadata.is_size_known = true;
    const auto updated = object.updated();
    if (updated != std::chrono::system_clock::time_point{})
    {
        const auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(updated.time_since_epoch()).count();
        metadata.last_modified = Poco::Timestamp(static_cast<Poco::Timestamp::TimeVal>(microseconds));
    }
    metadata.etag = object.etag();
    for (const auto & [key, value] : object.metadata())
        metadata.attributes.emplace(key, value);
    if (with_tags)
        metadata.tags = {};
    return metadata;
}

std::map<std::string, std::string> attributesToMap(const std::optional<ObjectAttributes> & attributes)
{
    std::map<std::string, std::string> result;
    if (!attributes)
        return result;
    for (const auto & [key, value] : *attributes)
        result.emplace(key, value);
    return result;
}

#if USE_AWS_S3
void applyGCSXMLMultipartUploadDefaults(S3::S3RequestSettings & request_settings)
{
    if (request_settings[S3RequestSetting::strict_upload_part_size] == S3::DEFAULT_STRICT_UPLOAD_PART_SIZE)
    {
        request_settings[S3RequestSetting::strict_upload_part_size] = 64 * 1024 * 1024;
        request_settings[S3RequestSetting::max_single_part_upload_size] = 64 * 1024 * 1024;
    }
}
#endif

void validateNativeGCSWriteSettings(const WriteSettings & write_settings)
{
    if (!write_settings.object_storage_write_if_match.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS object storage does not support object_storage_write_if_match");
    if (!write_settings.object_storage_write_if_none_match.empty() && write_settings.object_storage_write_if_none_match != "*")
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Native GCS object storage supports only '*' for object_storage_write_if_none_match, got '{}'",
            write_settings.object_storage_write_if_none_match);
}

String statusLogMessage(const GCS::Status & status)
{
    if (status.ok())
        return {};
    if (status.message.empty())
        return GCS::statusCodeName(status.code);
    return fmt::format("{}: {}", GCS::statusCodeName(status.code), status.message);
}

class GCSReadBuffer final : public ReadBufferFromFileBase
{
public:
    GCSReadBuffer(
        std::shared_ptr<GCS::HighLevelClient> high_level_client_,
        String bucket_,
        String object_name_,
        size_t buf_size,
        std::optional<size_t> file_size_,
        std::optional<size_t> read_hint_,
        bool remote_fs_prefetch_,
        size_t prefetch_buffer_size_,
        size_t remote_read_min_bytes_for_seek_,
        ThrottlerPtr remote_throttler_,
        BlobStorageLogWriterPtr blob_storage_log_)
        : ReadBufferFromFileBase(buf_size, nullptr, 0, file_size_)
        , high_level_client(std::move(high_level_client_))
        , bucket(std::move(bucket_))
        , object_name(std::move(object_name_))
        , read_hint(read_hint_)
        , remote_fs_prefetch(remote_fs_prefetch_)
        , prefetch_buffer_size(prefetch_buffer_size_)
        , remote_read_min_bytes_for_seek(remote_read_min_bytes_for_seek_)
        , remote_throttler(std::move(remote_throttler_))
        , blob_storage_log(std::move(blob_storage_log_))
    {
    }

    ~GCSReadBuffer() override
    {
        try
        {
            finishSequentialStream(/* require_expected_bytes */ false, /* ignore_close_errors */ true);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    String getFileName() const override { return object_name; }

    off_t seek(off_t off, int whence) override
    {
        off_t new_position = 0;
        if (whence == SEEK_SET)
            new_position = off;
        else if (whence == SEEK_CUR)
            new_position = getPosition() + off;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS read buffer supports only SEEK_SET and SEEK_CUR");

        if (new_position < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS read buffer cannot seek to negative offset {}", new_position);
        if (file_size && static_cast<size_t>(new_position) > *file_size)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Native GCS read buffer seek offset {} is past object size {}", new_position, *file_size);

        if (!working_buffer.empty() && read_offset >= working_buffer.size())
        {
            const size_t current_buffer_start = read_offset - working_buffer.size();
            if (static_cast<size_t>(new_position) >= current_buffer_start && static_cast<size_t>(new_position) < read_offset)
            {
                pos = working_buffer.end() - (read_offset - static_cast<size_t>(new_position));
                return getPosition();
            }
        }

        const off_t current_position = getPosition();
        if (sequential_stream && new_position > current_position)
        {
            const size_t diff = static_cast<size_t>(new_position - current_position);
            if (diff < remote_read_min_bytes_for_seek)
            {
                ignore(diff);
                return new_position;
            }
        }

        read_offset = static_cast<size_t>(new_position);
        resetSequentialStream();
        set(internal_buffer.begin(), internal_buffer.size());
        return new_position;
    }

    off_t getPosition() override { return static_cast<off_t>(read_offset - available()); }

    bool supportsReadAt() override { return true; }
    bool supportsRightBoundedReads() const override { return true; }

    size_t readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> & progress_callback) const override
    {
        if (file_size && offset >= *file_size)
            return 0;

        size_t bytes_to_read = n;
        if (file_size)
            bytes_to_read = std::min(bytes_to_read, *file_size - offset);

        return fetchRangeInto(to, offset, bytes_to_read, progress_callback);
    }

    std::optional<size_t> getRemoteFileSize() const override { return file_size; }
    size_t getFileOffsetOfBufferEnd() const override { return read_offset; }

    void setReadUntilPosition(size_t position) override
    {
        if (read_until_position && *read_until_position == position)
            return;

        resetSequentialStream();
        read_until_position = position;
        sequential_eof = read_offset >= position;
    }

    void setReadUntilEnd() override
    {
        if (!read_until_position)
            return;

        resetSequentialStream();
        read_until_position.reset();
        sequential_eof = false;
    }

private:
    using ReadObjectStream = google::cloud::storage::ObjectReadStream;

    struct SequentialStream
    {
        GCS::HighLevelReadResult result;
        std::optional<size_t> limit;
        size_t bytes_read_from_stream = 0;
    };

    size_t available() const { return static_cast<size_t>(working_buffer.end() - pos); }

    bool nextImpl() override
    {
        Stopwatch watch;
        Int32 error_code = 0;
        String error_message;
        size_t bytes_read = 0;

        try
        {
            CurrentThread::ReadThrottlingScope read_throttling_scope(remote_throttler);

            if (file_size && read_offset >= *file_size)
            {
                if (sequential_stream)
                    finishSequentialStream();
                return false;
            }
            while (bytes_read < internal_buffer.size() && !sequential_eof)
            {
                ensureSequentialStream(read_offset + bytes_read);
                if (sequential_eof)
                    break;

                const size_t bytes_to_read = internal_buffer.size() - bytes_read;
                const size_t stream_bytes = readFromHighLevelStream(
                    sequential_stream->result.stream, internal_buffer.begin() + bytes_read, bytes_to_read);
                if (stream_bytes == 0)
                {
                    finishSequentialStream();
                    continue;
                }

                sequential_stream->bytes_read_from_stream += stream_bytes;
                bytes_read += stream_bytes;
            }

            if (bytes_read == 0)
                return false;

            if (remote_throttler)
                remote_throttler->throttle(bytes_read);

            working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + bytes_read);
            pos = working_buffer.begin();
            read_offset += bytes_read;

            ProfileEvents::increment(ProfileEvents::ReadBufferFromGCSBytes, bytes_read);
            ProfileEvents::increment(ProfileEvents::ReadBufferFromGCSMicroseconds, watch.elapsedMicroseconds());
            addBlobLogEvent(bytes_read, watch.elapsedMicroseconds(), 0, {});
            return true;
        }
        catch (...)
        {
            if (!error_code)
            {
                error_code = getCurrentExceptionCode();
                error_message = getCurrentExceptionMessage(false);
                ProfileEvents::increment(ProfileEvents::ReadBufferFromGCSRequestsErrors);
                addBlobLogEvent(bytes_read, watch.elapsedMicroseconds(), error_code, error_message);
            }
            throw;
        }
    }

    std::optional<size_t> sequentialReadLimit(size_t offset) const
    {
        std::optional<size_t> limit;
        if (file_size)
            limit = std::min(*file_size - offset, sequentialWindowSizeForKnownFile());
        else
        {
            size_t estimated_limit = 0;
            if (read_hint)
                estimated_limit = std::max(estimated_limit, *read_hint);
            if (remote_fs_prefetch)
                estimated_limit = std::max(estimated_limit, prefetch_buffer_size);
            if (estimated_limit > internal_buffer.size())
                limit = estimated_limit;
        }

        if (read_until_position)
        {
            if (offset >= *read_until_position)
                return 0;
            const size_t bounded_limit = *read_until_position - offset;
            if (!limit || *limit > bounded_limit)
                limit = bounded_limit;
        }

        return limit;
    }

    size_t sequentialWindowSizeForKnownFile() const
    {
        size_t limit = std::max(internal_buffer.size(), internal_buffer.size() * 8);
        limit = std::max(limit, prefetch_buffer_size);
        if (read_hint)
            limit = std::max(limit, *read_hint);
        return limit;
    }

    void ensureSequentialStream(size_t offset)
    {
        if (sequential_stream || sequential_eof)
            return;

        if ((file_size && offset >= *file_size) || (read_until_position && offset >= *read_until_position))
        {
            sequential_eof = true;
            return;
        }

        auto limit = sequentialReadLimit(offset);
        if (limit && *limit == 0)
        {
            sequential_eof = true;
            return;
        }

        Stopwatch init_watch;
        sequential_stream.emplace();
        sequential_stream->limit = limit;
        sequential_stream->result = high_level_client->readObject(bucket, object_name, offset, limit);
        ProfileEvents::increment(ProfileEvents::ReadBufferFromGCSInitMicroseconds, init_watch.elapsedMicroseconds());

        if (!sequential_stream->result.ok())
        {
            auto status = sequential_stream->result.status;
            sequential_stream.reset();
            GCS::throwIfError(status, "ReadObject");
        }
    }

    void finishSequentialStream(bool require_expected_bytes = true, bool ignore_close_errors = false)
    {
        if (!sequential_stream)
            return;

        const auto bytes_read_from_stream = sequential_stream->bytes_read_from_stream;
        const auto limit = sequential_stream->limit;
        std::optional<size_t> bytes_to_discard_before_final_status;
        if (!ignore_close_errors && limit && bytes_read_from_stream < *limit)
            bytes_to_discard_before_final_status = *limit - bytes_read_from_stream;
        auto close_status = closeHighLevelStream(
            sequential_stream->result.stream, ignore_close_errors, bytes_to_discard_before_final_status);
        sequential_stream.reset();

        if (!close_status.ok() && !ignore_close_errors)
        {
            high_level_client->recordReadObjectFailure(close_status);
            GCS::throwIfError(close_status, "ReadObject");
        }

        if (require_expected_bytes && file_size && limit && bytes_read_from_stream < *limit)
            throw Exception(
                ErrorCodes::S3_ERROR,
                "Native GCS ReadObject for '{}' ended after {} bytes before requested {} bytes",
                object_name,
                bytes_read_from_stream,
                *limit);

        if (file_size)
        {
            sequential_eof = read_offset >= *file_size;
            return;
        }
        if (!limit || bytes_read_from_stream < *limit)
            sequential_eof = true;
    }

    size_t readFromHighLevelStream(ReadObjectStream & stream, char * to, size_t limit) const
    {
        if (limit == 0)
            return 0;

        stream.read(to, static_cast<std::streamsize>(limit));
        const auto bytes_read = static_cast<size_t>(stream.gcount());
        auto status = GCS::fromCloudStatus(stream.status());
        if (!status.ok())
        {
            high_level_client->recordReadObjectFailure(status);
            GCS::throwIfError(status, "ReadObject");
        }
        return bytes_read;
    }

    static GCS::Status drainHighLevelStreamStatus(ReadObjectStream & stream, std::optional<size_t> bytes_to_discard_before_final_status)
    {
        std::array<char, DBMS_DEFAULT_BUFFER_SIZE> discard_buffer{};
        while (stream.IsOpen() && bytes_to_discard_before_final_status && *bytes_to_discard_before_final_status > 0)
        {
            const size_t bytes_to_read = std::min(discard_buffer.size(), *bytes_to_discard_before_final_status);
            stream.read(discard_buffer.data(), static_cast<std::streamsize>(bytes_to_read));
            auto status = GCS::fromCloudStatus(stream.status());
            if (!status.ok())
                return status;
            const size_t bytes_read = static_cast<size_t>(stream.gcount());
            if (bytes_read == 0)
                return status;
            *bytes_to_discard_before_final_status -= bytes_read;
        }

        char extra_byte = 0;
        while (stream.IsOpen())
        {
            stream.read(&extra_byte, 1);
            auto status = GCS::fromCloudStatus(stream.status());
            if (!status.ok())
                return status;
            if (stream.gcount() != 0)
                return GCS::makeStatus(GCS::StatusCode::InvalidArgument, "Native GCS ReadObject returned bytes after requested range");
            if (!stream.good())
                return status;
        }
        return GCS::fromCloudStatus(stream.status());
    }

    static GCS::Status closeHighLevelStream(
        ReadObjectStream & stream, bool ignore_close_errors, std::optional<size_t> bytes_to_discard_before_final_status = std::nullopt)
    {
        GCS::Status close_status;
        try
        {
            if (bytes_to_discard_before_final_status)
            {
                close_status = drainHighLevelStreamStatus(stream, bytes_to_discard_before_final_status);
                if (!close_status.ok())
                    return close_status;
            }
            if (stream.IsOpen())
                stream.Close();
            close_status = GCS::fromCloudStatus(stream.status());
        }
        catch (...)
        {
            close_status = GCS::fromCloudStatus(stream.status());
            if (close_status.ok() && !ignore_close_errors)
                throw;
        }
        return close_status;
    }

    void resetSequentialStream()
    {
        try
        {
            finishSequentialStream(/* require_expected_bytes */ false);
        }
        catch (...)
        {
            const auto error_code = getCurrentExceptionCode();
            const auto error_message = getCurrentExceptionMessage(false);
            ProfileEvents::increment(ProfileEvents::ReadBufferFromGCSRequestsErrors);
            addBlobLogEvent(0, 0, error_code, error_message);
            throw;
        }
        resetSequentialState();
    }

    void resetSequentialState()
    {
        pending_data.clear();
        pending_data_offset = 0;
        sequential_eof = false;
    }

    void addBlobLogEvent(size_t data_size, size_t elapsed_microseconds, Int32 code, const String & message) const
    {
        if (blob_storage_log)
            blob_storage_log->addEvent(
                BlobStorageLogElement::EventType::Read, bucket, object_name, {}, data_size, elapsed_microseconds, code, message);
    }

    size_t fetchRangeInto(
        char * to, size_t offset, size_t limit, const std::function<bool(size_t)> & progress_callback) const
    {
        if (limit == 0)
            return 0;

        Stopwatch watch;
        Int32 error_code = 0;
        String error_message;
        size_t bytes_read = 0;

        try
        {
            CurrentThread::ReadThrottlingScope read_throttling_scope(remote_throttler);

            Stopwatch init_watch;
            auto stream_result = high_level_client->readObject(bucket, object_name, offset, limit);
            ProfileEvents::increment(ProfileEvents::ReadBufferFromGCSInitMicroseconds, init_watch.elapsedMicroseconds());
            if (!stream_result.ok())
            {
                error_code = GCS::errorCodeForStatus(stream_result.status.code);
                error_message = statusLogMessage(stream_result.status);
                ProfileEvents::increment(ProfileEvents::ReadBufferFromGCSRequestsErrors);
                addBlobLogEvent(bytes_read, watch.elapsedMicroseconds(), error_code, error_message);
                GCS::throwIfError(stream_result.status, "ReadObject");
            }

            bool cancelled = false;
            while (bytes_read < limit && !cancelled)
            {
                const size_t bytes_to_read = std::min(internal_buffer.size(), limit - bytes_read);
                const size_t stream_bytes = readFromHighLevelStream(stream_result.stream, to + bytes_read, bytes_to_read);
                if (stream_bytes == 0)
                    break;

                bytes_read += stream_bytes;
                if (progress_callback && progress_callback(bytes_read))
                    cancelled = true;
            }

            std::optional<size_t> bytes_to_discard_before_final_status;
            if (!cancelled && bytes_read < limit)
                bytes_to_discard_before_final_status = limit - bytes_read;
            auto finish_status = closeHighLevelStream(stream_result.stream, cancelled, bytes_to_discard_before_final_status);
            if (!finish_status.ok() && !cancelled)
            {
                error_code = GCS::errorCodeForStatus(finish_status.code);
                error_message = statusLogMessage(finish_status);
                ProfileEvents::increment(ProfileEvents::ReadBufferFromGCSRequestsErrors);
                addBlobLogEvent(bytes_read, watch.elapsedMicroseconds(), error_code, error_message);
                high_level_client->recordReadObjectFailure(finish_status);
                GCS::throwIfError(finish_status, "ReadObject");
            }

            if (remote_throttler && bytes_read)
                remote_throttler->throttle(bytes_read);

            ProfileEvents::increment(ProfileEvents::ReadBufferFromGCSBytes, bytes_read);
            ProfileEvents::increment(ProfileEvents::ReadBufferFromGCSMicroseconds, watch.elapsedMicroseconds());
            addBlobLogEvent(bytes_read, watch.elapsedMicroseconds(), 0, {});
            return bytes_read;
        }
        catch (...)
        {
            if (!error_code)
            {
                error_code = getCurrentExceptionCode();
                error_message = getCurrentExceptionMessage(false);
                ProfileEvents::increment(ProfileEvents::ReadBufferFromGCSRequestsErrors);
                addBlobLogEvent(bytes_read, watch.elapsedMicroseconds(), error_code, error_message);
            }
            throw;
        }
    }

    std::shared_ptr<GCS::HighLevelClient> high_level_client;
    String bucket;
    String object_name;
    std::optional<size_t> read_hint;
    std::optional<size_t> read_until_position;
    bool remote_fs_prefetch;
    size_t prefetch_buffer_size;
    size_t remote_read_min_bytes_for_seek;
    ThrottlerPtr remote_throttler;
    BlobStorageLogWriterPtr blob_storage_log;
    std::optional<SequentialStream> sequential_stream;
    String pending_data;
    size_t pending_data_offset = 0;
    size_t read_offset = 0;
    bool sequential_eof = false;
};

class GCSWriteBuffer final : public WriteBufferFromFileBase
{
public:
    GCSWriteBuffer(
        std::shared_ptr<GCS::HighLevelClient> high_level_client_,
        String bucket_,
        String object_name_,
        std::optional<ObjectAttributes> attributes_,
        size_t buf_size,
        bool allow_parallel_upload_,
        String object_storage_write_if_none_match_,
        ThrottlerPtr remote_throttler_,
        BlobStorageLogWriterPtr blob_storage_log_)
        : WriteBufferFromFileBase(buf_size, nullptr, 0)
        , high_level_client(std::move(high_level_client_))
        , bucket(std::move(bucket_))
        , object_name(std::move(object_name_))
        , attributes(std::move(attributes_))
        , allow_parallel_upload(allow_parallel_upload_)
        , object_storage_write_if_none_match(std::move(object_storage_write_if_none_match_))
        , parallel_write_threshold(std::max<size_t>(buf_size * 2, max_write_chunk_bytes))
        , upload_id(nextUploadId())
        , remote_throttler(std::move(remote_throttler_))
        , blob_storage_log(std::move(blob_storage_log_))
    {
    }

    ~GCSWriteBuffer() override
    {
        if (upload_finalized)
            return;

        try
        {
            waitForTemporaryUploads();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        try
        {
            cleanupTemporaryObjects();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    std::string getFileName() const override { return object_name; }

    void sync() override
    {
        explicit_sync_flush = true;
        try
        {
            next();
            if (parallel_mode)
                flushParallelStagedData(/* force */ false);
            explicit_sync_flush = false;
        }
        catch (...)
        {
            explicit_sync_flush = false;
            throw;
        }
    }

private:
    static constexpr size_t max_write_chunk_bytes = google::storage::v2::ServiceConstants::MAX_WRITE_CHUNK_BYTES;
    static constexpr size_t max_compose_sources = 32;
    static constexpr size_t max_concurrent_uploads = 7;

    static UInt64 nextUploadId()
    {
        static std::atomic_uint64_t counter{1};
        return counter.fetch_add(1, std::memory_order_relaxed);
    }

    void nextImpl() override
    {
        const size_t size = offset();
        if (!size)
            return;

        staged_data.append(working_buffer.begin(), size);
        if (!allow_parallel_upload || (!parallel_mode && staged_data.size() <= parallel_write_threshold))
            return;

        startParallelMode();
        flushParallelStagedData(explicit_sync_flush);
    }

    void finalizeImpl() override
    {
        try
        {
            if (offset())
                staged_data.append(working_buffer.begin(), offset());

            if (!parallel_mode && (!allow_parallel_upload || staged_data.size() <= parallel_write_threshold))
            {
                writeObjectPayload(object_name, staged_data, attributesToMap(attributes), ifGenerationMatchZero(), /* record_failure */ true);
                staged_data.clear();
                recordUploadElapsed();
                addUploadBlobLogEvent(0, {});
                upload_finalized = true;
                return;
            }

            startParallelMode();
            flushParallelStagedData(/* force */ true);

            waitForTemporaryUploads();
            composeTemporaryObjects();
            cleanupTemporaryObjects();
            recordUploadElapsed();
            addUploadBlobLogEvent(0, {});
            upload_finalized = true;
        }
        catch (...)
        {
            const auto error_code = getCurrentExceptionCode();
            const auto error_message = getCurrentExceptionMessage(false);
            try
            {
                cleanupTemporaryObjects();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
            recordUploadFailure(error_code, error_message);
            throw;
        }
    }

    void startParallelMode()
    {
        parallel_mode = true;
    }

    void flushParallelStagedData(bool force)
    {
        while (staged_data.size() >= max_write_chunk_bytes || (force && !staged_data.empty()))
        {
            const size_t part_size = force ? std::min(max_write_chunk_bytes, staged_data.size()) : max_write_chunk_bytes;
            String payload(staged_data.data(), part_size);
            staged_data.erase(0, part_size);
            scheduleTemporaryUpload(std::move(payload));
        }
    }

    bool ifGenerationMatchZero() const
    {
        return object_storage_write_if_none_match == "*";
    }

    String makeTemporaryObjectName(std::string_view kind, size_t index) const
    {
        return fmt::format("{}.clickhouse-gcs-compose-tmp/{}/{}-{}", object_name, upload_id, kind, index);
    }

    void scheduleTemporaryUpload(String payload)
    {
        while (upload_futures.size() >= max_concurrent_uploads)
            waitForOneTemporaryUpload();

        const auto part_number = next_part_number++;
        String temporary_object = makeTemporaryObjectName("part", part_number);
        temporary_sources.push_back(temporary_object);

        upload_futures.push_back(std::async(
            std::launch::async,
            [this, temporary_object, upload_payload = std::move(payload)]
            {
                writeObjectPayload(temporary_object, upload_payload, {}, /* if_generation_match_zero */ true, /* record_failure */ false);
                markTemporaryObjectCreated(temporary_object);
            }));
    }

    void markTemporaryObjectCreated(const String & object)
    {
        std::lock_guard lock(temporary_objects_mutex);
        temporary_objects.push_back(object);
    }

    void waitForOneTemporaryUpload()
    {
        auto future = std::move(upload_futures.front());
        upload_futures.erase(upload_futures.begin());
        future.get();
    }

    void waitForTemporaryUploads()
    {
        std::exception_ptr first_exception;
        for (auto & future : upload_futures)
        {
            try
            {
                future.get();
            }
            catch (...)
            {
                if (!first_exception)
                    first_exception = std::current_exception();
            }
        }
        upload_futures.clear();
        if (first_exception)
            std::rethrow_exception(first_exception);
    }

    void composeTemporaryObjects()
    {
        if (temporary_sources.empty())
        {
            writeObjectPayload(object_name, {}, attributesToMap(attributes), ifGenerationMatchZero(), /* record_failure */ true);
            return;
        }

        composeObjects(temporary_sources, object_name, /* final_object */ true);
    }

    std::vector<String> composeObjects(const std::vector<String> & sources, const String & destination, bool final_object)
    {
        if (sources.size() <= max_compose_sources)
        {
            composeObjectsOnce(sources, destination, final_object);
            return {destination};
        }

        std::vector<String> next_level;
        for (size_t offset = 0; offset < sources.size(); offset += max_compose_sources)
        {
            const size_t end = std::min(offset + max_compose_sources, sources.size());
            std::vector<String> group(sources.begin() + offset, sources.begin() + end);
            String intermediate = makeTemporaryObjectName("compose", next_compose_number++);
            composeObjectsOnce(group, intermediate, /* final_object */ false);
            markTemporaryObjectCreated(intermediate);
            next_level.push_back(std::move(intermediate));
        }

        return composeObjects(next_level, destination, final_object);
    }

    void composeObjectsOnce(const std::vector<String> & sources, const String & destination, bool final_object)
    {
        auto result = high_level_client->composeObject(
            bucket,
            sources,
            destination,
            final_object ? attributesToMap(attributes) : std::map<std::string, std::string>{},
            final_object ? ifGenerationMatchZero() : true);
        if (!result.status.ok())
        {
            if (final_object)
                recordUploadFailure(GCS::errorCodeForStatus(result.status.code), statusLogMessage(result.status));
            GCS::throwIfError(result.status, "ComposeObject");
        }
    }

    void cleanupTemporaryObjects()
    {
        std::vector<String> objects_to_delete;
        {
            std::lock_guard lock(temporary_objects_mutex);
            objects_to_delete.swap(temporary_objects);
        }

        for (auto it = objects_to_delete.rbegin(); it != objects_to_delete.rend(); ++it)
        {
            auto status = high_level_client->deleteObject(bucket, *it);
            if (status.code == GCS::StatusCode::NotFound)
                continue;
            GCS::throwIfError(status, "DeleteObject temporary parallel GCS write object");
        }
    }

    void writeObjectPayload(
        const String & target_object,
        const String & payload,
        const std::map<std::string, std::string> & metadata,
        bool if_generation_match_zero,
        bool record_failure)
    {
        auto result = high_level_client->insertObject(bucket, target_object, payload, metadata, if_generation_match_zero);
        if (!result.status.ok())
        {
            if (record_failure)
                recordUploadFailure(GCS::errorCodeForStatus(result.status.code), statusLogMessage(result.status));
            GCS::throwIfError(result.status, "InsertObject");
        }

        accountAcceptedPayload(payload.size());
    }

    void accountAcceptedPayload(size_t chunk_size)
    {
        if (!chunk_size)
            return;

        if (remote_throttler)
            remote_throttler->throttle(chunk_size);

        ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSBytes, chunk_size);
        std::lock_guard lock(accepted_payload_mutex);
        accepted_payload_bytes += chunk_size;
    }

    void recordUploadElapsed()
    {
        if (!upload_elapsed_recorded)
        {
            ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSMicroseconds, upload_watch.elapsedMicroseconds());
            upload_elapsed_recorded = true;
        }
    }

    void addUploadBlobLogEvent(Int32 error_code, const String & error_message)
    {
        if (!blob_storage_log || upload_blob_log_written)
            return;

        blob_storage_log->addEvent(
            BlobStorageLogElement::EventType::Upload,
            bucket,
            object_name,
            {},
            accepted_payload_bytes,
            upload_watch.elapsedMicroseconds(),
            error_code,
            error_message);
        upload_blob_log_written = true;
    }

    void recordUploadFailure(Int32 error_code, const String & error_message)
    {
        if (upload_failure_recorded)
            return;

        upload_failure_recorded = true;
        ProfileEvents::increment(ProfileEvents::WriteBufferFromGCSRequestsErrors);
        recordUploadElapsed();
        addUploadBlobLogEvent(error_code, error_message);
    }

    std::shared_ptr<GCS::HighLevelClient> high_level_client;
    String bucket;
    String object_name;
    std::optional<ObjectAttributes> attributes;
    bool allow_parallel_upload;
    String object_storage_write_if_none_match;
    size_t parallel_write_threshold;
    UInt64 upload_id;
    ThrottlerPtr remote_throttler;
    BlobStorageLogWriterPtr blob_storage_log;
    Stopwatch upload_watch;
    std::vector<std::future<void>> upload_futures;
    std::vector<String> temporary_sources;
    std::vector<String> temporary_objects;
    std::mutex temporary_objects_mutex;
    std::mutex accepted_payload_mutex;
    String staged_data;
    bool explicit_sync_flush = false;
    bool parallel_mode = false;
    bool upload_finalized = false;
    bool upload_elapsed_recorded = false;
    bool upload_blob_log_written = false;
    bool upload_failure_recorded = false;
    size_t next_part_number = 0;
    size_t next_compose_number = 0;
    size_t accepted_payload_bytes = 0;
};

}
#endif


#if USE_GOOGLE_CLOUD && USE_AWS_S3
GCSObjectStorage::GCSObjectStorage(
    GCSObjectStorageSettings settings_,
    std::shared_ptr<GCS::HighLevelClient> high_level_client_,
    std::shared_ptr<const S3::Client> xml_multipart_client_)
    : settings(std::move(settings_))
    , high_level_client(std::move(high_level_client_))
    , xml_multipart_client(std::move(xml_multipart_client_))
{
}
#elif USE_GOOGLE_CLOUD
GCSObjectStorage::GCSObjectStorage(GCSObjectStorageSettings settings_, std::shared_ptr<GCS::HighLevelClient> high_level_client_)
    : settings(std::move(settings_))
    , high_level_client(std::move(high_level_client_))
{
}
#else
GCSObjectStorage::GCSObjectStorage(GCSObjectStorageSettings settings_)
    : settings(std::move(settings_))
{
}
#endif

bool GCSObjectStorage::exists(const StoredObject & object) const
{
#if USE_GOOGLE_CLOUD
    return tryGetObjectMetadata(object.remote_path, /* with_tags */ false).has_value();
#else
    (void)object;
    throwNotImplemented("exists");
#endif
}

std::unique_ptr<ReadBufferFromFileBase>
GCSObjectStorage::readObject(const StoredObject & object, const ReadSettings & read_settings, std::optional<size_t> read_hint) const
{
#if USE_GOOGLE_CLOUD
    std::optional<size_t> file_size;
    if (object.bytes_size != std::numeric_limits<uint64_t>::max())
        file_size = static_cast<size_t>(object.bytes_size);

    BlobStorageLogWriterPtr blob_storage_log;
    if (read_settings.enable_blob_storage_log_for_read_operations)
    {
        blob_storage_log = createBlobStorageLogWriter();
        if (blob_storage_log)
            blob_storage_log->local_path = object.local_path;
    }

    return std::make_unique<GCSReadBuffer>(
        high_level_client,
        settings.bucket,
        objectName(object.remote_path),
        read_settings.remote_fs_buffer_size,
        file_size,
        read_hint,
        read_settings.remote_fs_prefetch,
        read_settings.prefetch_buffer_size,
        read_settings.remote_read_min_bytes_for_seek,
        read_settings.remote_throttler,
        std::move(blob_storage_log));
#else
    (void)object;
    (void)read_settings;
    throwNotImplemented("readObject");
#endif
}

std::unique_ptr<WriteBufferFromFileBase> GCSObjectStorage::writeObject(
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    const WriteSettings & write_settings)
{
#if USE_GOOGLE_CLOUD
    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS object storage supports only WriteMode::Rewrite");
    if (settings.read_only)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS object storage disk '{}' is read-only", settings.disk_name);
    validateNativeGCSWriteSettings(write_settings);
    if (settings.write_transport == GCS::WriteTransport::XMLMultipart)
    {
#if USE_AWS_S3
        if (!write_settings.object_storage_write_if_none_match.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Native GCS XML multipart write transport does not support object_storage_write_if_none_match until create-if-absent equivalence is validated");
        if (!xml_multipart_client)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Native GCS XML multipart client is not initialized for disk '{}'", settings.disk_name);

        S3::S3RequestSettings request_settings = settings.xml_request_settings;
        applyGCSXMLMultipartUploadDefaults(request_settings);
        if (auto query_context = CurrentThread::tryGetQueryContext();
            query_context && !query_context->isBackgroundContext())
        {
            const auto & query_settings = query_context->getSettingsRef();
            request_settings.updateFromSettings(
                query_settings,
                /* if_changed */ true,
                query_settings[Setting::s3_validate_request_settings]);
        }

        ThreadPoolCallbackRunnerUnsafe<void> scheduler;
        if (write_settings.s3_allow_parallel_part_upload)
            scheduler = threadPoolCallbackRunnerUnsafe<void>(getThreadPoolWriter(), ThreadName::REMOTE_FS_WRITE_THREAD_POOL);

        auto disk_write_settings = IObjectStorage::patchSettings(write_settings);
        auto blob_storage_log = createBlobStorageLogWriter();
        if (blob_storage_log)
            blob_storage_log->local_path = object.local_path;

        return std::make_unique<WriteBufferFromS3>(
            xml_multipart_client,
            settings.bucket,
            objectName(object.remote_path),
            write_settings.use_adaptive_write_buffer ? write_settings.adaptive_write_buffer_initial_size : buf_size,
            request_settings,
            std::move(blob_storage_log),
            std::move(attributes),
            std::move(scheduler),
            disk_write_settings,
            S3::ProfileEventsNamespace::GCS);
#else
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Native GCS XML multipart write transport requires ClickHouse to be built with S3/XML multipart support (USE_AWS_S3)");
#endif
    }

    auto blob_storage_log = createBlobStorageLogWriter();
    if (blob_storage_log)
        blob_storage_log->local_path = object.local_path;

    return std::make_unique<GCSWriteBuffer>(
        high_level_client,
        settings.bucket,
        objectName(object.remote_path),
        std::move(attributes),
        write_settings.use_adaptive_write_buffer ? write_settings.adaptive_write_buffer_initial_size : buf_size,
        write_settings.s3_allow_parallel_part_upload,
        write_settings.object_storage_write_if_none_match,
        write_settings.remote_throttler,
        std::move(blob_storage_log));
#else
    (void)object;
    (void)mode;
    (void)attributes;
    (void)buf_size;
    (void)write_settings;
    throwNotImplemented("writeObject");
#endif
}

void GCSObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    auto blob_storage_log = createBlobStorageLogWriter();
    removeObjectIfExistsImpl(object, blob_storage_log);
}

void GCSObjectStorage::removeObjectIfExistsImpl(const StoredObject & object, const BlobStorageLogWriterPtr & blob_storage_log) const
{
#if USE_GOOGLE_CLOUD
    Stopwatch watch;
    auto status = high_level_client->deleteObject(settings.bucket, objectName(object.remote_path));
    if (blob_storage_log)
        blob_storage_log->addEvent(
            BlobStorageLogElement::EventType::Delete,
            settings.bucket,
            object.remote_path,
            object.local_path,
            object.bytes_size,
            watch.elapsedMicroseconds(),
            status.ok() ? 0 : GCS::errorCodeForStatus(status.code),
            statusLogMessage(status));

    if (status.code == GCS::StatusCode::NotFound)
        return;
    GCS::throwIfError(status, "DeleteObject");
#else
    (void)object;
    (void)blob_storage_log;
    throwNotImplemented("removeObjectIfExists");
#endif
}

void GCSObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    auto blob_storage_log = createBlobStorageLogWriter();
    for (const auto & object : objects)
        removeObjectIfExistsImpl(object, blob_storage_log);
}

#if USE_GOOGLE_CLOUD
bool GCSObjectStorage::isCompatibleForNativeRewriteFrom(const GCSObjectStorage & source_storage) const
{
    const auto & destination_settings = settings.client_settings;
    const auto & source_settings = source_storage.settings.client_settings;
    return destination_settings.endpoint == source_settings.endpoint
        && GCS::credentialMode(destination_settings) == GCS::credentialMode(source_settings)
        && destination_settings.service_account_json == source_settings.service_account_json
        && destination_settings.user_project == source_settings.user_project
        && destination_settings.use_insecure_credentials_for_tests == source_settings.use_insecure_credentials_for_tests;
}

void GCSObjectStorage::rewriteObjectFromGCS(
    const GCSObjectStorage & source_storage,
    const StoredObject & object_from,
    const StoredObject & object_to,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> object_to_attributes) const
{
    if (settings.read_only)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS object storage disk '{}' is read-only", settings.disk_name);

    validateNativeGCSWriteSettings(write_settings);

    auto result = high_level_client->rewriteObject(
        source_storage.settings.bucket,
        objectName(object_from.remote_path),
        settings.bucket,
        objectName(object_to.remote_path),
        attributesToMap(object_to_attributes),
        write_settings.object_storage_write_if_none_match == "*");
    GCS::throwIfError(result.status, "RewriteObject");
}
#endif

void GCSObjectStorage::copyObject(
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> object_to_attributes)
{
#if USE_GOOGLE_CLOUD
    (void)read_settings;
    rewriteObjectFromGCS(*this, object_from, object_to, write_settings, std::move(object_to_attributes));
#else
    auto in = readObject(object_from, read_settings);
    auto out = writeObject(object_to, WriteMode::Rewrite, std::move(object_to_attributes), DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    copyData(*in, *out);
    out->finalize();
#endif
}

void GCSObjectStorage::copyObjectToAnotherObjectStorage(
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    if (&object_storage_to == this)
    {
        copyObject(object_from, object_to, read_settings, write_settings, std::move(object_to_attributes));
        return;
    }

#if USE_GOOGLE_CLOUD
    if (auto * destination_gcs = dynamic_cast<GCSObjectStorage *>(&object_storage_to);
        destination_gcs != nullptr && destination_gcs->isCompatibleForNativeRewriteFrom(*this))
    {
        (void)read_settings;
        destination_gcs->rewriteObjectFromGCS(*this, object_from, object_to, write_settings, std::move(object_to_attributes));
        return;
    }
#endif

    auto in = readObject(object_from, read_settings);
    auto out = object_storage_to.writeObject(
        object_to, WriteMode::Rewrite, std::move(object_to_attributes), DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    copyData(*in, *out);
    out->finalize();
}

ObjectMetadata GCSObjectStorage::getObjectMetadata(const std::string & path, bool with_tags) const
{
    auto metadata = tryGetObjectMetadata(path, with_tags);
    if (!metadata)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Native GCS object '{}' does not exist", path);
    return *metadata;
}

std::optional<ObjectMetadata> GCSObjectStorage::tryGetObjectMetadata(const std::string & path, bool with_tags) const
{
#if USE_GOOGLE_CLOUD
    auto result = high_level_client->getObjectMetadata(settings.bucket, objectName(path));
    if (result.status.code == GCS::StatusCode::NotFound)
        return std::nullopt;
    GCS::throwIfError(result.status, "GetObjectMetadata");
    return metadataFromCloud(result.response, with_tags);
#else
    (void)path;
    (void)with_tags;
    throwNotImplemented("tryGetObjectMetadata");
#endif
}

void GCSObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
#if USE_GOOGLE_CLOUD
    const size_t target_keys = max_keys;
    auto result = high_level_client->listObjects(settings.bucket, objectName(path), max_keys, std::nullopt);
    GCS::throwIfError(result.status, "ListObjects");

    for (const auto & object : result.response)
    {
        children.emplace_back(std::make_shared<RelativePathWithMetadata>(object.name(), metadataFromCloud(object, /* with_tags */ false)));
        if (target_keys && children.size() >= target_keys)
            return;
    }
#else
    (void)path;
    (void)children;
    (void)max_keys;
    throwNotImplemented("listObjects");
#endif
}

ObjectStorageIteratorPtr GCSObjectStorage::iterate(
    const std::string & path_prefix, size_t max_keys, bool with_tags, const std::optional<std::string> & start_after) const
{
#if USE_GOOGLE_CLOUD
    RelativePathsWithMetadata files;
    const size_t fetch_max_keys = start_after && !start_after->empty() && max_keys ? max_keys + 1 : max_keys;
    auto result = high_level_client->listObjects(settings.bucket, objectName(path_prefix), fetch_max_keys, start_after);
    GCS::throwIfError(result.status, "ListObjects");

    for (const auto & object : result.response)
    {
        if (start_after && !start_after->empty() && object.name() <= *start_after)
            continue;

        files.emplace_back(std::make_shared<RelativePathWithMetadata>(object.name(), metadataFromCloud(object, with_tags)));
        if (max_keys && files.size() >= max_keys)
            return std::make_shared<ObjectStorageIteratorFromList>(std::move(files));
    }

    return std::make_shared<ObjectStorageIteratorFromList>(std::move(files));
#else
    (void)path_prefix;
    (void)max_keys;
    (void)with_tags;
    (void)start_after;
    throwNotImplemented("iterate");
#endif
}

ReadSettings GCSObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    return read_settings;
}

WriteSettings GCSObjectStorage::patchSettings(const WriteSettings & write_settings) const
{
    return write_settings;
}

ObjectStorageKeyGeneratorPtr GCSObjectStorage::createKeyGenerator() const
{
    return createObjectStorageKeyGeneratorByPrefix(settings.key_prefix);
}

BlobStorageLogWriterPtr GCSObjectStorage::createBlobStorageLogWriter() const
{
    if (settings.blob_storage_log_writer_factory)
        return settings.blob_storage_log_writer_factory(settings.disk_name);
    return BlobStorageLogWriter::create(settings.disk_name);
}

void GCSObjectStorage::throwNotImplemented(std::string_view operation) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Native GCS object storage operation '{}' is not implemented before the core read/write disk phase",
        operation);
}

GCSObjectStorageSettings getGCSObjectStorageSettings(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const ContextPtr & context)
{
    if (!config.has(config_prefix + ".bucket"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS object storage requires `bucket` in config");

    GCSObjectStorageSettings settings;
    settings.disk_name = name;
    settings.bucket = expandConfigString(config, config_prefix + ".bucket", context);
    if (settings.bucket.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS object storage bucket cannot be empty");

    if (config.has(config_prefix + ".key_prefix"))
        settings.key_prefix = normalizeKeyPrefix(expandConfigString(config, config_prefix + ".key_prefix", context));

    if (config.has(config_prefix + ".endpoint"))
        settings.client_settings.endpoint = expandConfigString(config, config_prefix + ".endpoint", context);
    if (settings.client_settings.endpoint.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS object storage endpoint cannot be empty");

    settings.client_settings.request_timeout_ms
        = config.getUInt64(config_prefix + ".request_timeout_ms", settings.client_settings.request_timeout_ms);
    settings.client_settings.max_retry_attempts
        = config.getUInt64(config_prefix + ".max_retry_attempts", settings.client_settings.max_retry_attempts);
    settings.client_settings.service_account_json = config.getString(config_prefix + ".service_account_json", "");
    settings.client_settings.user_project = config.getString(config_prefix + ".user_project", "");
    settings.client_settings.use_insecure_credentials_for_tests
        = config.getBool(config_prefix + ".use_insecure_credentials_for_tests", false);
    settings.client_settings.for_disk = true;
    settings.write_transport = GCS::parseWriteTransport(config.getString(config_prefix + ".write_transport", GCS::writeTransportName(GCS::WriteTransport::Grpc)));
    if (config.has(config_prefix + ".xml_endpoint"))
        settings.xml_endpoint = expandConfigString(config, config_prefix + ".xml_endpoint", context);
    if (settings.write_transport == GCS::WriteTransport::XMLMultipart)
    {
        GCS::assertXMLMultipartSupportAvailable();
        settings.xml_client_settings = GCS::makeXMLMultipartClientSettings(settings.client_settings, settings.xml_endpoint);
#if USE_AWS_S3
        settings.xml_request_settings = S3::S3RequestSettings(
            config,
            context->getSettingsRef(),
            config_prefix,
            "xml_multipart.",
            context->getSettingsRef()[Setting::s3_validate_request_settings]);
#endif
    }

    const auto get_request_throttler_max_speed = config.getUInt64(config_prefix + ".get_request_throttler_max_speed", 0);
    if (get_request_throttler_max_speed)
        settings.client_settings.request_throttler.get_throttler = std::make_shared<Throttler>(
            get_request_throttler_max_speed,
            ProfileEvents::GCSGetRequestThrottlerCount,
            ProfileEvents::GCSGetRequestThrottlerSleepMicroseconds);

    const auto put_request_throttler_max_speed = config.getUInt64(config_prefix + ".put_request_throttler_max_speed", 0);
    if (put_request_throttler_max_speed)
        settings.client_settings.request_throttler.put_throttler = std::make_shared<Throttler>(
            put_request_throttler_max_speed,
            ProfileEvents::GCSPutRequestThrottlerCount,
            ProfileEvents::GCSPutRequestThrottlerSleepMicroseconds);

    settings.read_only = config.getBool(config_prefix + ".read_only", config.getBool(config_prefix + ".readonly", false));
    settings.description = fmt::format("{}/{}", settings.client_settings.endpoint, settings.bucket);

    return settings;
}

ObjectStoragePtr createGCSObjectStorage(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const ContextPtr & context,
    bool)
{
    auto settings = getGCSObjectStorageSettings(name, config, config_prefix, context);
    GCS::assertGrpcAvailable();

#if USE_GOOGLE_CLOUD
    auto high_level_client = GCS::createHighLevelClient(settings.client_settings);
#if USE_AWS_S3
    std::shared_ptr<const S3::Client> xml_multipart_client;
    if (settings.write_transport == GCS::WriteTransport::XMLMultipart)
    {
        auto token_provider = GCS::createXMLBearerTokenProvider(settings.client_settings);
        auto unique_xml_client = GCS::createXMLMultipartClient(
            settings.client_settings,
            settings.xml_endpoint,
            context,
            settings.disk_name,
            token_provider);
        xml_multipart_client = std::shared_ptr<S3::Client>(std::move(unique_xml_client));
    }
    return std::make_shared<GCSObjectStorage>(std::move(settings), std::move(high_level_client), std::move(xml_multipart_client));
#else
    return std::make_shared<GCSObjectStorage>(std::move(settings), std::move(high_level_client));
#endif
#else
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Native GCS gRPC support is not available because ClickHouse was built without Google Cloud C++ gRPC support");
#endif
}

}
