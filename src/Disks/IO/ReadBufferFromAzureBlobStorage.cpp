#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Common/BlobStorageLogWriter.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <IO/ReadBufferFromString.h>
#include <IO/AzureBlobStorage/PocoHTTPClient.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>
#include <Common/Throttler.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/ProfileEvents.h>
#include <IO/SeekableReadBuffer.h>
#include <base/sleep.h>

#include <limits>
#include <optional>


namespace ProfileEvents
{
    extern const Event ReadBufferFromAzureMicroseconds;
    extern const Event ReadBufferFromAzureBytes;
    extern const Event ReadBufferFromAzureRequestsErrors;
    extern const Event AzureGetObject;
    extern const Event DiskAzureGetObject;
    extern const Event ReadBufferFromAzureInitMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int RECEIVED_EMPTY_DATA;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int NOT_INITIALIZED;
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int HTTP_RANGE_NOT_SATISFIABLE;
    extern const int FILE_CHANGED_DURING_READ;
}

namespace
{

/// A successful `Download` is not enough to trust the body: the endpoint may have ignored the
/// requested range and answered `200 OK` with the whole object from byte 0, or `206 Partial
/// Content` for a different range. Consuming such a body as if it started at `requested_offset`
/// would hand the caller the wrong bytes under the right offsets - silent data corruption - so
/// the start of the returned range is checked against the requested one before the body is read.
/// The SDK reports a `200 OK` response as the range starting at 0, so a full-object response is
/// accepted exactly when the request started at 0, where it is a correct answer, the same as in
/// `ReadWriteBufferFromHTTP`.
void checkReturnedRange(const Azure::Storage::Blobs::Models::DownloadBlobResult & result, size_t requested_offset, const String & path)
{
    if (result.ContentRange.Offset != static_cast<int64_t>(requested_offset))
        throw Exception(ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE,
            "Azure Blob Storage returned a range starting at offset {} instead of the requested offset {} for file {}",
            result.ContentRange.Offset, requested_offset, path);
}

/// Pins a download to the generation of the object that was selected at read setup. Without it a
/// blob that is overwritten in place between two requests of the same logical read - the first
/// `Download` and a reopen after a premature end of the response, or a retry - would hand the
/// caller bytes stitched together from two different objects. The expected tag usually comes from
/// a listing, where it is bare, while `If-Match` takes the quoted entity-tag form.
void setExpectedETag(Azure::Storage::Blobs::DownloadBlobOptions & download_options, const String & expected_etag)
{
    if (!expected_etag.empty())
        download_options.AccessConditions.IfMatch = Azure::ETag(AzureBlobStorage::toQuotedETag(expected_etag));
}

/// Defence in depth for an endpoint that ignores `If-Match` and answers with the new generation
/// anyway. An empty `ETag` in the response means the endpoint said nothing about the generation,
/// which cannot be compared with anything.
void checkReturnedETag(const Azure::Storage::Blobs::Models::DownloadBlobResult & result, const String & expected_etag, const String & path)
{
    if (expected_etag.empty())
        return;

    /// The listing spells the tag bare and the response header spells it quoted, so the two are
    /// compared by their opaque part - see `normalizeETag`.
    const String response_etag = AzureBlobStorage::getETagOrEmpty(result.Details.ETag);
    if (response_etag.empty() || AzureBlobStorage::normalizeETag(response_etag) == AzureBlobStorage::normalizeETag(expected_etag))
        return;

    throw Exception(ErrorCodes::FILE_CHANGED_DURING_READ,
        "Azure Blob Storage object {} was replaced during read (etag changed from {} to {})",
        path, expected_etag, response_etag);
}

/// The `If-Match` precondition was evaluated by the endpoint and failed: the object is no longer
/// the one the read started from. That is not a transient error, so it must not be retried.
void rethrowIfObjectChanged(const Azure::Core::RequestFailedException & e, const String & expected_etag, const String & path)
{
    if (expected_etag.empty() || e.StatusCode != Azure::Core::Http::HttpStatusCode::PreconditionFailed)
        return;

    throw Exception(ErrorCodes::FILE_CHANGED_DURING_READ,
        "Azure Blob Storage object {} was replaced during read (If-Match on etag {} failed)",
        path, expected_etag);
}

}

ReadBufferFromAzureBlobStorage::ReadBufferFromAzureBlobStorage(
    ContainerClientPtr blob_container_client_,
    const String & path_,
    const ReadSettings & read_settings_,
    size_t max_single_read_retries_,
    size_t max_single_download_retries_,
    bool use_external_buffer_,
    bool restricted_seek_,
    size_t read_until_position_,
    BlobStorageLogWriterPtr blob_storage_log_,
    String container_for_logging_,
    std::optional<size_t> known_object_size_,
    String expected_etag_)
    : ReadBufferFromFileBase()
    , blob_container_client(blob_container_client_)
    , path(path_)
    , max_single_read_retries(max_single_read_retries_)
    , max_single_download_retries(max_single_download_retries_)
    , read_settings(read_settings_)
    , tmp_buffer_size(read_settings.remote_fs_settings.buffer_size)
    , use_external_buffer(use_external_buffer_)
    , restricted_seek(restricted_seek_)
    , read_until_position(read_until_position_)
    , known_object_size(known_object_size_)
    , expected_etag(std::move(expected_etag_))
    , last_object_metadata(std::make_unique<std::optional<ObjectMetadata>>())
    , blob_storage_log(std::move(blob_storage_log_))
    , container_for_logging(std::move(container_for_logging_))
{
    if (!use_external_buffer)
    {
        tmp_buffer.resize(tmp_buffer_size);
        data_ptr = tmp_buffer.data();
        data_capacity = tmp_buffer_size;
    }
}

void ReadBufferFromAzureBlobStorage::setReadUntilEnd()
{
    if (read_until_position)
    {
        read_until_position = 0;
        if (initialized)
        {
            offset = getPosition();
            resetWorkingBuffer();
            initialized = false;
        }
    }
}

void ReadBufferFromAzureBlobStorage::setReadUntilPosition(size_t position)
{
    read_until_position = position;
    initialized = false;
}

bool ReadBufferFromAzureBlobStorage::nextImpl()
{
    if (read_until_position)
    {
        if (read_until_position == offset)
            return false;

        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);
    }

    if (!initialized)
        initialize(/* attempt */ 0);

    if (use_external_buffer)
    {
        data_ptr = internal_buffer.begin();
        data_capacity = internal_buffer.size();
    }

    size_t to_read_bytes = std::min(static_cast<size_t>(total_size - offset), data_capacity);
    size_t bytes_read = 0;

    size_t sleep_time_with_backoff_milliseconds = 100;
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadBufferFromAzureMicroseconds);

    for (size_t i = 0; i < max_single_read_retries; ++i)
    {
        bool premature_end_of_response = false;
        try
        {
            ResourceGuard rlock(ResourceGuard::Metrics::getIORead(), read_settings.io_scheduling.read_resource_link, to_read_bytes);
            bytes_read = data_stream->ReadToCount(reinterpret_cast<uint8_t *>(data_ptr), to_read_bytes);
            rlock.unlock(bytes_read); // Do not hold resource under bandwidth throttler
            if (read_settings.remote_throttler)
                read_settings.remote_throttler->throttle(bytes_read);

            if (bytes_read != 0)
                break;

            /// The body of the current response is exhausted. That is the end of the file only if
            /// the response delivered everything it was supposed to deliver - see `getEndOfData`.
            /// An endpoint that caps a request to a shorter response would otherwise silently
            /// truncate the file at the end of that response body.
            if (static_cast<size_t>(offset) >= getEndOfData())
                break;

            premature_end_of_response = true;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromAzureRequestsErrors);
            LOG_DEBUG(log, "Exception caught during Azure Read for file {} at attempt {}/{}: {}", path, i + 1, max_single_read_retries, e.Message);

            if (i + 1 == max_single_read_retries || !isRetryableAzureException(e))
                throw;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
            initialized = false;
            initialize(i + 1);
        }
        catch (...)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromAzureRequestsErrors);
            LOG_DEBUG(log, "Exception caught during Azure Read for file {} at attempt {}/{}: {}", path, i + 1, max_single_read_retries, getCurrentExceptionMessage(false));
            /// It doesn't make sense to retry allocator errors
            if (getCurrentExceptionCode() == ErrorCodes::CANNOT_ALLOCATE_MEMORY)
                throw;

            if (i + 1 == max_single_read_retries)
                throw;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
            initialized = false;
            initialize(i + 1);
        }

        if (premature_end_of_response)
        {
            /// The response ended before the end of the data - see `getEndOfData`. That is not a
            /// valid end of the file, so a shorter response must not be reported to the caller as
            /// one: reopen the download at the current offset, and if the endpoint keeps answering
            /// short, fail instead of returning truncated data.
            const size_t end_of_data = getEndOfData();

            ProfileEvents::increment(ProfileEvents::ReadBufferFromAzureRequestsErrors);
            LOG_DEBUG(log, "Premature end of the response at offset {} while reading until position {} for file {} at attempt {}/{}",
                offset, end_of_data, path, i + 1, max_single_read_retries);

            if (i + 1 == max_single_read_retries)
                throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE,
                    "Premature end of the response from Azure Blob Storage at offset {} while reading until position {} of file {}",
                    offset, end_of_data, path);

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
            initialized = false;
            initialize(i + 1);
        }
    }


    if (bytes_read == 0)
        return false;

    ProfileEvents::increment(ProfileEvents::ReadBufferFromAzureBytes, bytes_read);
    BufferBase::set(data_ptr, bytes_read, 0);

    offset += bytes_read;

    return true;
}

off_t ReadBufferFromAzureBlobStorage::seek(off_t offset_, int whence)
{
    if (offset_ == getPosition() && whence == SEEK_SET)
        return offset_;

    if (initialized && restricted_seek)
    {
        throw Exception(
            ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
            "Seek is allowed only before first read attempt from the buffer (current offset: "
            "{}, new offset: {}, reading until position: {}, available: {})",
            getPosition(), offset_, read_until_position, available());
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
            chassert(pos >= working_buffer.begin());
            chassert(pos < working_buffer.end());

            return getPosition();
        }

        off_t position = getPosition();
        if (initialized && offset_ > position)
        {
            size_t diff = offset_ - position;
            if (diff < read_settings.remote_fs_settings.min_bytes_for_seek)
            {
                ignore(diff);
                return offset_;
            }
        }

        resetWorkingBuffer();
        if (initialized)
            initialized = false;
    }

    /// A seek starts a new logical read, so the lower bound on the object size learnt from the
    /// responses of the previous one does not carry over.
    reported_object_size = 0;
    offset = offset_;
    return offset;
}

off_t ReadBufferFromAzureBlobStorage::getPosition()
{
    return offset - available();
}

size_t ReadBufferFromAzureBlobStorage::getEndOfData() const
{
    /// `read_until_position` is set locally by the caller, so it is authoritative in both
    /// directions: neither more nor less data than it asks for may reach the caller.
    if (read_until_position)
        return static_cast<size_t>(read_until_position);

    /// For an unbounded read the size that the object had when it was listed or headed - before
    /// this read started - is the next best bound: it does not come from the response that is
    /// being validated, and the download is pinned to that same generation of the object with
    /// `If-Match` whenever an `ETag` is known.
    if (known_object_size)
        return *known_object_size;

    /// Nothing is known locally. The size of the object advertised by the download response
    /// itself (`Content-Range`) is the only statement about where the data ends. It is remote
    /// data, so it is only used as a lower bound: a response that ends before it is treated as a
    /// premature end of the response, while a response that goes past it is read to its real end.
    return reported_object_size;
}

void ReadBufferFromAzureBlobStorage::initialize(size_t attempt)
{
    if (initialized)
        return;

    Azure::Storage::Blobs::DownloadBlobOptions download_options;

    Azure::Nullable<int64_t> length {};
    if (read_until_position != 0)
        length = {static_cast<int64_t>(read_until_position - offset)};

    download_options.Range = {static_cast<int64_t>(offset), length};
    setExpectedETag(download_options, expected_etag);

    Azure::Core::Context azure_context = Azure::Core::Context().WithValue(PocoAzureHTTPClient::getSDKContextKeyForBufferRetry(), attempt);

    if (!blob_client)
        blob_client = std::make_unique<Azure::Storage::Blobs::BlobClient>(blob_container_client->GetBlobClient(path));

    size_t sleep_time_with_backoff_milliseconds = 100;
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadBufferFromAzureInitMicroseconds);

    for (size_t i = 0; i < max_single_download_retries; ++i)
    {
        /// Measures time-to-first-byte: just the `Download` API call, not data transfer.
        /// Each download attempt is logged individually as a separate `Read` event.
        Stopwatch blob_log_watch;
        try
        {
            ProfileEvents::increment(ProfileEvents::AzureGetObject);
            if (blob_container_client->IsClientForDisk())
                ProfileEvents::increment(ProfileEvents::DiskAzureGetObject);

            auto download_response = blob_client->Download(download_options, azure_context);
            checkReturnedRange(download_response.Value, offset, path);
            checkReturnedETag(download_response.Value, expected_etag, path);

            setMetadataFromResponse(download_response.Value.Details, download_response.Value.BlobSize);
            data_stream = std::move(download_response.Value.BodyStream);
            /// Only ever grows within one logical read: a later response that advertises a smaller
            /// object than an earlier one did must not lower the bound again, or an endpoint whose
            /// `Content-Range` totals shrink between reopens would get a premature end of the
            /// response accepted as the end of the file after all.
            reported_object_size = std::max(reported_object_size, static_cast<size_t>(download_response.Value.BlobSize));

            /// Defence in depth: the body stream is optional in the SDK, and everything below
            /// dereferences it, starting with `data_stream->Length()` in the log event. Check it
            /// here, before the first dereference, rather than only after the retry loop.
            if (!data_stream)
                throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA,
                    "Null data stream obtained while downloading file {} from Blob Storage", path);

            if (blob_storage_log)
            {
                blob_storage_log->addEvent(
                    BlobStorageLogElement::EventType::Read,
                    /* bucket */ container_for_logging, /* remote_path */ path, /* local_path */ {},
                    /* data_size */ static_cast<size_t>(data_stream->Length()),
                    blob_log_watch.elapsedMicroseconds(),
                    /* error_code */ 0, /* error_message */ {});
            }
            break;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            if (blob_storage_log)
            {
                blob_storage_log->addEvent(
                    BlobStorageLogElement::EventType::Read,
                    /* bucket */ container_for_logging, /* remote_path */ path, /* local_path */ {},
                    length.HasValue() ? static_cast<size_t>(length.Value()) : 0,
                    blob_log_watch.elapsedMicroseconds(),
                    static_cast<Int32>(e.StatusCode), e.Message);
            }

            ProfileEvents::increment(ProfileEvents::ReadBufferFromAzureRequestsErrors);
            LOG_DEBUG(log, "Exception caught during Azure Download for file {} at offset {} at attempt {}/{}: {}", path, offset, i + 1, max_single_download_retries, e.Message);

            rethrowIfObjectChanged(e, expected_etag, path);

            if (i + 1 == max_single_download_retries || !isRetryableAzureException(e))
                throw;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
        catch (...)
        {
            if (blob_storage_log)
            {
                blob_storage_log->addEvent(
                    BlobStorageLogElement::EventType::Read,
                    /* bucket */ container_for_logging, /* remote_path */ path, /* local_path */ {},
                    length.HasValue() ? static_cast<size_t>(length.Value()) : 0,
                    blob_log_watch.elapsedMicroseconds(),
                    static_cast<Int32>(getCurrentExceptionCode()), getCurrentExceptionMessage(false));
            }

            ProfileEvents::increment(ProfileEvents::ReadBufferFromAzureRequestsErrors);
            LOG_DEBUG(log, "Exception caught during Azure Download for file {} at attempt {}/{}: {}", path, i + 1, max_single_download_retries, getCurrentExceptionMessage(false));
            /// It doesn't make sense to retry allocator errors
            if (getCurrentExceptionCode() == ErrorCodes::CANNOT_ALLOCATE_MEMORY)
                throw;

            if (i + 1 == max_single_download_retries)
                throw;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
    }

    if (data_stream == nullptr)
        throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "Null data stream obtained while downloading file {} from Blob Storage", path);

    /// The offset just past the last byte that the current download is allowed to deliver.
    /// Only `read_until_position`, which is set locally by the caller, is a trustworthy bound:
    /// when it is set, it is authoritative in both directions. An endpoint that answers a ranged
    /// request with more data than was requested must not be able to push bytes past the right
    /// bound into the caller, and an endpoint that answers with less must not be able to move the
    /// end of the file before the right bound either (`nextImpl` reopens the download or throws
    /// on a premature end of the response instead).
    ///
    /// The size that the object had when it was listed or headed is trustworthy for the same
    /// reason, and pinning the download to that generation with `If-Match` keeps it applicable to
    /// every request of the read.
    ///
    /// When neither is available, the `Content-Length` of the response, chosen by the remote
    /// endpoint, is deliberately not consulted: a length that under-reports the body would
    /// otherwise turn into a hard end of the file and silently truncate the data. The actual end
    /// of the data is then wherever the response body actually ends.
    if (read_until_position)
        total_size = static_cast<size_t>(read_until_position);
    else if (known_object_size)
        total_size = *known_object_size;
    else
        total_size = std::numeric_limits<size_t>::max();

    initialized = true;
}

std::optional<size_t> ReadBufferFromAzureBlobStorage::tryGetFileSize()
{
    if (!blob_client)
        blob_client = std::make_unique<Azure::Storage::Blobs::BlobClient>(blob_container_client->GetBlobClient(path));

    if (!file_size)
        file_size = blob_client->GetProperties().Value.BlobSize;

    return file_size;
}

std::optional<RemoteFileMetadata> ReadBufferFromAzureBlobStorage::getRemoteFileMetadata() const
{
    const auto properties = blob_container_client->GetBlobClient(path).GetProperties().Value;
    const auto last_modification_time = std::chrono::duration_cast<std::chrono::seconds>(
        static_cast<std::chrono::system_clock::time_point>(properties.LastModified).time_since_epoch())
        .count();
    return RemoteFileMetadata{
        .size = static_cast<size_t>(properties.BlobSize),
        .last_modification_time = static_cast<time_t>(last_modification_time)};
}

size_t copyFromAzureBodyStream(Azure::Core::IO::BodyStream & body_stream, char * to, size_t n, const Azure::Core::Context & context)
{
    /// The length of the body reported by the remote endpoint is deliberately not consulted: it
    /// can be larger than the destination buffer, which only has room for `n` bytes, and it can
    /// also undercut the bytes the body can actually produce, which must not truncate the copy.
    /// `ReadToCount` stops at the actual end of the body, so the size of the destination is the
    /// only bound that is needed.
    return body_stream.ReadToCount(reinterpret_cast<uint8_t *>(to), n, context);
}

size_t ReadBufferFromAzureBlobStorage::readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & /*progress_callback*/) const
{
    size_t initial_n = n;
    size_t sleep_time_with_backoff_milliseconds = 100;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ReadBufferFromAzureMicroseconds);

    /// `supportsReadAt` allows a positioned read on a freshly constructed buffer, which has not
    /// created `blob_client` yet: the member is only created by the sequential path and by
    /// `tryGetFileSize`. Positioned reads also run concurrently on the same buffer, so the member
    /// must not be created here either. Getting a blob client is a local operation, so a
    /// call-local one is used whenever the shared one does not exist yet.
    std::optional<AzureBlobStorage::BlobClient> local_blob_client;
    if (!blob_client)
        local_blob_client.emplace(blob_container_client->GetBlobClient(path));
    const AzureBlobStorage::BlobClient & client = blob_client ? *blob_client : *local_blob_client;

    for (size_t i = 0; i < max_single_download_retries && n > 0; ++i)
    {
        size_t bytes_copied = 0;
        Stopwatch blob_log_watch;

        try
        {
            ProfileEvents::increment(ProfileEvents::AzureGetObject);
            if (blob_container_client->IsClientForDisk())
                ProfileEvents::increment(ProfileEvents::DiskAzureGetObject);

            Azure::Storage::Blobs::DownloadBlobOptions download_options;
            download_options.Range = {static_cast<int64_t>(range_begin), n};
            setExpectedETag(download_options, expected_etag);
            Azure::Core::Context azure_context = Azure::Core::Context().WithValue(PocoAzureHTTPClient::getSDKContextKeyForBufferRetry(), size_t{0});

            auto download_response = client.Download(download_options, azure_context);
            checkReturnedRange(download_response.Value, range_begin, path);
            checkReturnedETag(download_response.Value, expected_etag, path);

            if (blob_storage_log)
            {
                blob_storage_log->addEvent(
                    BlobStorageLogElement::EventType::Read,
                    /* bucket */ container_for_logging, /* remote_path */ path, /* local_path */ {},
                    n,
                    blob_log_watch.elapsedMicroseconds(),
                    /* error_code */ 0, /* error_message */ {});
            }

            setMetadataFromResponse(download_response.Value.Details, download_response.Value.BlobSize);

            std::unique_ptr<Azure::Core::IO::BodyStream> body_stream = std::move(download_response.Value.BodyStream);
            /// Defence in depth, the same as in `initialize`: the body stream is optional in the
            /// SDK and must not be dereferenced blindly.
            if (!body_stream)
                throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA,
                    "Null data stream obtained while downloading file {} from Blob Storage", path);

            bytes_copied = copyFromAzureBodyStream(*body_stream, to, n, azure_context);

            LOG_TEST(log, "AzureBlobStorage readBigAt read bytes {}", bytes_copied);

            if (read_settings.remote_throttler)
                read_settings.remote_throttler->throttle(bytes_copied);
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            if (blob_storage_log)
            {
                blob_storage_log->addEvent(
                    BlobStorageLogElement::EventType::Read,
                    /* bucket */ container_for_logging, /* remote_path */ path, /* local_path */ {},
                    n,
                    blob_log_watch.elapsedMicroseconds(),
                    static_cast<Int32>(e.StatusCode), e.Message);
            }

            ProfileEvents::increment(ProfileEvents::ReadBufferFromAzureRequestsErrors);
            LOG_DEBUG(log, "Exception caught during Azure Download for file {} at offset {} at attempt {}/{}: {}", path, offset, i + 1, max_single_download_retries, e.Message);

            rethrowIfObjectChanged(e, expected_etag, path);

            if (i + 1 == max_single_download_retries || !isRetryableAzureException(e))
                throw;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
        catch (...)
        {
            if (blob_storage_log)
            {
                blob_storage_log->addEvent(
                    BlobStorageLogElement::EventType::Read,
                    /* bucket */ container_for_logging, /* remote_path */ path, /* local_path */ {},
                    n,
                    blob_log_watch.elapsedMicroseconds(),
                    static_cast<Int32>(getCurrentExceptionCode()), getCurrentExceptionMessage(false));
            }

            ProfileEvents::increment(ProfileEvents::ReadBufferFromAzureRequestsErrors);
            LOG_DEBUG(log, "Exception caught during Azure Download for file {} at attempt {}/{}: {}", path, i + 1, max_single_download_retries, getCurrentExceptionMessage(false));
            /// It doesn't make sense to retry allocator errors
            if (getCurrentExceptionCode() == ErrorCodes::CANNOT_ALLOCATE_MEMORY)
                throw;

            if (i + 1 == max_single_download_retries)
                throw;

            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }


        ProfileEvents::increment(ProfileEvents::ReadBufferFromAzureBytes, bytes_copied);

        range_begin += bytes_copied;
        to += bytes_copied;
        n -= bytes_copied;
    }

    if (n > 0)
    {
        /// The endpoint kept returning short responses. Report how much was actually copied:
        /// the caller must not treat the tail of its buffer as initialized. `readBigAt` is
        /// documented to stop at the end of the file and return the number of bytes read, and the
        /// callers that cannot accept a short read already turn it into `UNEXPECTED_END_OF_FILE`,
        /// so this is not thrown here - but exhausting the retry budget is not normal, hence the
        /// warning.
        LOG_WARNING(log, "AzureBlobStorage readBigAt for file {} got only {} bytes out of {} requested after {} attempts",
            path, initial_n - n, initial_n, max_single_download_retries);
    }

    return initial_n - n;
}

ObjectMetadata ReadBufferFromAzureBlobStorage::getObjectMetadataFromTheLastRequest() const
{
    if (!last_object_metadata.get()->has_value())
        throw Exception(ErrorCodes::NOT_INITIALIZED, "No Azure object metadata available because there were no successful requests");

    return last_object_metadata.get()->value();
}

void ReadBufferFromAzureBlobStorage::setMetadataFromResponse(const Azure::Storage::Blobs::Models::DownloadBlobDetails & details, size_t blob_size) const
{
    ObjectMetadata new_metadata;
    new_metadata.size_bytes = blob_size;
    new_metadata.etag = AzureBlobStorage::getETagOrEmpty(details.ETag);
    new_metadata.last_modified = static_cast<std::chrono::system_clock::time_point>(details.LastModified).time_since_epoch().count();
    if (!details.Metadata.empty())
    {
        for (const auto & [key, value] : details.Metadata)
            new_metadata.attributes[key] = value;
    }

    last_object_metadata.set(std::make_unique<std::optional<ObjectMetadata>>(std::move(new_metadata)));
}

}

#endif
