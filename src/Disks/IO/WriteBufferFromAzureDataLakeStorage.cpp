#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <base/sleep.h>

#include <Poco/URI.h>

#include <Disks/IO/WriteBufferFromAzureDataLakeStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>

#include <azure/core/io/body_stream.hpp>
#include <azure/storage/files/datalake/datalake_options.hpp>


namespace ProfileEvents
{
    extern const Event AzureUpload;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int AZURE_BLOB_STORAGE_ERROR;
}

namespace
{

Azure::Storage::Files::DataLake::DataLakeClientOptions toDataLakeOptions(
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options)
{
    Azure::Storage::Files::DataLake::DataLakeClientOptions out;
    static_cast<Azure::Core::_internal::ClientOptions &>(out)
        = static_cast<const Azure::Core::_internal::ClientOptions &>(blob_client_options);
    return out;
}

String prefixedBlobPath(const AzureBlobStorage::Endpoint & endpoint, const String & blob_path_)
{
    String full = endpoint.prefix;
    if (!full.empty() && !full.ends_with('/'))
        full += '/';
    full += blob_path_;
    return full;
}

/// Object keys are limited to 1024 bytes (see StorageObjectStorageSink::validateKey).
constexpr size_t MAX_BLOB_KEY_SIZE = 1024;
/// How many staging names to try before giving up, in case one is already taken.
constexpr size_t MAX_STAGING_NAME_ATTEMPTS = 8;
constexpr size_t STAGING_SUFFIX_LENGTH = 16;

/// Errors after which the service state is unknown, so a mutating request must not be repeated.
/// ClickHouse's Azure transport never throws: it synthesizes 408 for a timeout and 500 for any
/// other transport failure (see PocoAzureHTTPClient::makeRequestInternalImpl), which is why 408
/// belongs here even though isRetryableAzureException excludes it. This is deliberately not that
/// helper: it also treats 403 as retryable during access checks, but a 403 is a definite answer.
bool isAmbiguousAzureStatus(Azure::Core::Http::HttpStatusCode status)
{
    return status == Azure::Core::Http::HttpStatusCode::RequestTimeout
        || status >= Azure::Core::Http::HttpStatusCode::InternalServerError;
}

/// Appended to an authorization failure. The service's own error is preserved as-is: which permission
/// is missing is not something we can tell from a 403.
const char * authorizationHint()
{
    return ". Writing an object stages it as a sibling and renames it onto the target, so the credential"
           " must be allowed to create and to rename within the directory";
}

}

String makeAdlsGen2StagingPath(const String & blob_path_, const String & random_suffix)
{
    /// A sibling of the final path, so directory-scoped credentials keep working.
    const String suffix = ".tmp." + random_suffix;
    if (suffix.size() >= MAX_BLOB_KEY_SIZE)
        return {};

    if (blob_path_.size() + suffix.size() <= MAX_BLOB_KEY_SIZE)
        return blob_path_ + suffix;

    /// Only the basename may be shortened. Cutting by byte offset alone can land before the last
    /// separator, which would put staging in another directory, and the sibling property is what keeps
    /// directory-scoped credentials working.
    const size_t slash = blob_path_.rfind('/');
    const size_t parent_size = slash == String::npos ? 0 : slash + 1;
    if (parent_size + suffix.size() >= MAX_BLOB_KEY_SIZE)
        return {};

    /// The suffix is what makes the name unique, so keep all of it and shorten the basename, stopping at
    /// a UTF-8 character boundary because keys must stay valid UTF-8.
    size_t basename_size = MAX_BLOB_KEY_SIZE - parent_size - suffix.size();
    while (basename_size > 0 && (static_cast<UInt8>(blob_path_[parent_size + basename_size]) & 0xC0) == 0x80)
        --basename_size;
    if (basename_size == 0)
        return {};
    return blob_path_.substr(0, parent_size + basename_size) + suffix;
}

String buildAdlsGen2FileUrl(const AzureBlobStorage::Endpoint & endpoint, const String & blob_path_)
{
    Poco::URI uri(endpoint.getContainerEndpoint());

    /// OneLake serves reads from the Blob host but writes only from the DFS host,
    /// so retarget the Fabric Blob host to DFS for the write client.
    const String blob_suffix = ".blob.fabric.microsoft.com";
    const String dfs_suffix = ".dfs.fabric.microsoft.com";
    String host = uri.getHost();
    if (host.ends_with(blob_suffix))
        uri.setHost(host.substr(0, host.size() - blob_suffix.size()) + dfs_suffix);

    String path = uri.getPath();
    if (!path.empty() && path.back() != '/')
        path += '/';
    path += prefixedBlobPath(endpoint, blob_path_);
    uri.setPath(path);
    return uri.toString();
}

constexpr size_t ADLFS_MAX_RETRIES = 10;

bool isAdlsGen2Endpoint(const AzureBlobStorage::Endpoint & endpoint)
{
    /// Only real Microsoft Fabric / OneLake hosts, matched at a label boundary.
    const String host = Poco::URI(endpoint.storage_account_url).getHost();
    return host.ends_with(".dfs.fabric.microsoft.com") || host.ends_with(".blob.fabric.microsoft.com");
}

Azure::Storage::Files::DataLake::DataLakeFileClient makeAdlsGen2FileClient(
    const AzureBlobStorage::Endpoint & endpoint,
    const AzureBlobStorage::AuthMethod & auth_method,
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options,
    const String & blob_path)
{
    using namespace Azure::Storage::Files::DataLake;
    auto datalake_options = toDataLakeOptions(blob_client_options);

    if (!endpoint.sas_auth.empty() || !endpoint.additional_params.empty())
        return DataLakeFileClient(buildAdlsGen2FileUrl(endpoint, blob_path), datalake_options);

    return std::visit(
        [&]<typename T>(const T & auth) -> DataLakeFileClient
        {
            if constexpr (std::is_same_v<T, AzureBlobStorage::ConnectionString>)
            {
                return DataLakeFileClient::CreateFromConnectionString(
                    auth.toUnderType(),
                    endpoint.container_name,
                    prefixedBlobPath(endpoint, blob_path),
                    datalake_options);
            }
            else
            {
                return DataLakeFileClient(buildAdlsGen2FileUrl(endpoint, blob_path), auth, datalake_options);
            }
        },
        auth_method);
}

namespace
{

/// Container URL for the filesystem client, retargeted to the DFS host like buildAdlsGen2FileUrl does,
/// because OneLake serves reads from the Blob host but writes only from DFS.
String buildAdlsGen2FileSystemUrl(const AzureBlobStorage::Endpoint & endpoint)
{
    Poco::URI uri(endpoint.getContainerEndpoint());

    const String blob_suffix = ".blob.fabric.microsoft.com";
    const String dfs_suffix = ".dfs.fabric.microsoft.com";
    String host = uri.getHost();
    if (host.ends_with(blob_suffix))
        uri.setHost(host.substr(0, host.size() - blob_suffix.size()) + dfs_suffix);

    return uri.toString();
}

/// The container's own path inside the endpoint, as DataLakeFileSystemClient sees it. RenameFile
/// otherwise derives the destination file system from the FIRST path segment, which is only correct
/// when the URL is exactly <host>/<container> and not <host>/<account>/<container>.
String adlsGen2FileSystemPath(
    const AzureBlobStorage::Endpoint & endpoint,
    const AzureBlobStorage::AuthMethod & auth_method)
{
    /// With a connection string the client appends only the container name to the service URL, and
    /// storage_account_url holds the connection string itself, so it cannot be parsed as a URL.
    if (endpoint.sas_auth.empty() && endpoint.additional_params.empty()
        && std::holds_alternative<AzureBlobStorage::ConnectionString>(auth_method))
        return endpoint.container_name;

    String path = Poco::URI(buildAdlsGen2FileSystemUrl(endpoint)).getPath();
    while (path.starts_with('/'))
        path.erase(0, 1);
    return path;
}

}

Azure::Storage::Files::DataLake::DataLakeFileSystemClient makeAdlsGen2FileSystemClient(
    const AzureBlobStorage::Endpoint & endpoint,
    const AzureBlobStorage::AuthMethod & auth_method,
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options,
    bool disable_sdk_retries)
{
    using namespace Azure::Storage::Files::DataLake;
    auto datalake_options = toDataLakeOptions(blob_client_options);
    /// Neither the guarded flush nor the rename is idempotent, so for those a lost response must not
    /// be re-sent behind our back.
    if (disable_sdk_retries)
        datalake_options.Retry.MaxRetries = 0;

    if (!endpoint.sas_auth.empty() || !endpoint.additional_params.empty())
        return DataLakeFileSystemClient(buildAdlsGen2FileSystemUrl(endpoint), datalake_options);

    return std::visit(
        [&]<typename T>(const T & auth) -> DataLakeFileSystemClient
        {
            if constexpr (std::is_same_v<T, AzureBlobStorage::ConnectionString>)
            {
                return DataLakeFileSystemClient::CreateFromConnectionString(
                    auth.toUnderType(),
                    endpoint.container_name,
                    datalake_options);
            }
            else
            {
                return DataLakeFileSystemClient(buildAdlsGen2FileSystemUrl(endpoint), auth, datalake_options);
            }
        },
        auth_method);
}

WriteBufferFromAzureDataLakeStorage::WriteBufferFromAzureDataLakeStorage(
    const AzureBlobStorage::Endpoint & endpoint_,
    const AzureBlobStorage::AuthMethod & auth_method_,
    const Azure::Storage::Blobs::BlobClientOptions & blob_client_options_,
    const String & blob_path_,
    size_t buf_size_,
    const WriteSettings & write_settings_,
    std::shared_ptr<const AzureBlobStorage::RequestSettings> settings_,
    const String & container_for_logging_,
    BlobStorageLogWriterPtr blob_log_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , log(getLogger("WriteBufferFromAzureDataLakeStorage"))
    , staging_fs_client(makeAdlsGen2FileSystemClient(endpoint_, auth_method_, blob_client_options_, /*disable_sdk_retries=*/ false))
    , publish_client(makeAdlsGen2FileSystemClient(endpoint_, auth_method_, blob_client_options_, /*disable_sdk_retries=*/ true))
    , publish_filesystem_path(adlsGen2FileSystemPath(endpoint_, auth_method_))
    , blob_path(blob_path_)
    , prefixed_blob_path(prefixedBlobPath(endpoint_, blob_path_))
    , write_settings(write_settings_)
    , max_unexpected_write_error_retries(settings_->max_unexpected_write_error_retries)
    , container_for_logging(container_for_logging_)
    , blob_log(std::move(blob_log_))
{
}

WriteBufferFromAzureDataLakeStorage::~WriteBufferFromAzureDataLakeStorage()
{
    /// `WriteBuffer::finalize` cancels on failure, but a caller that throws between `next` and
    /// `finalize`, or simply drops the buffer, never gets there. Same shape as
    /// WriteBufferFromS3::~WriteBufferFromS3 aborting its multipart upload.
    if (file_created && !published)
    {
        LOG_WARNING(log, "WriteBufferFromAzureDataLakeStorage for `{}` was neither published nor cleaned up, "
                    "removing the staging object in the destructor.", blob_path);
        cleanupStaging();
    }

    if (publish_outcome_unknown)
    {
        /// Reporting the file as unchanged here would contradict the exception `publish` just threw.
        LOG_INFO(log, "Outcome of publishing ADLS Gen2 file `{}` is unknown. It holds either its previous "
                 "contents or the new ones, never a partial object.", blob_path);
    }
    else if (published && (canceled || !finalized))
    {
        /// `preFinalize` is public and publishes before `finalized` is set, so a caller may cancel or
        /// drop the buffer after the target was already replaced. Reporting it as unchanged or unwritten
        /// would describe the opposite of what happened. A clean `finalize` stays silent, as before.
        LOG_INFO(log, "WriteBufferFromAzureDataLakeStorage for `{}` was {} after publishing. The file "
                 "holds the new contents.",
                 blob_path, canceled ? "canceled" : "dropped without being finalized");
    }
    else if (canceled)
    {
        LOG_INFO(log, "WriteBufferFromAzureDataLakeStorage was canceled. File `{}` is unchanged.", blob_path);
    }
    else if (!finalized)
    {
        LOG_INFO(log, "WriteBufferFromAzureDataLakeStorage is not finalized in destructor. File `{}` was not written to ADLS Gen2.", blob_path);
    }
}

void WriteBufferFromAzureDataLakeStorage::buildStagingClients()
{
    file_client.emplace(staging_fs_client.GetFileClient(staging_path));
    flush_client.emplace(publish_client.GetFileClient(staging_path));
}

void WriteBufferFromAzureDataLakeStorage::runWithRetries(
    const std::function<void()> & op,
    const char * what,
    BlobStorageLogElement::EventType event_type,
    size_t data_size)
{
    auto log_event = [&](Int32 error_code, const String & error_message, size_t elapsed_us)
    {
        if (blob_log)
            blob_log->addEvent(
                event_type,
                /* bucket */ container_for_logging,
                /* remote_path */ blob_path,
                /* local_path */ {},
                /* data_size */ data_size,
                /* elapsed_microseconds */ elapsed_us,
                error_code,
                error_message);
    };

    Stopwatch watch;
    size_t backoff_ms = 100;
    for (size_t attempt = 1; attempt < ADLFS_MAX_RETRIES; ++attempt)
    {
        LOG_TRACE(log, "ADLS Gen2 {} attempt {} for `{}`", what, attempt, blob_path);
        try
        {
            op();
            log_event(/*error_code=*/ 0, /*error_message=*/ {}, watch.elapsedMicroseconds());
            LOG_TRACE(log, "ADLS Gen2 {} attempt {} for `{}` succeeded", what, attempt, blob_path);
            return;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            const bool retryable = isRetryableAzureException(e);
            if (!retryable || attempt >= max_unexpected_write_error_retries)
            {
                log_event(static_cast<Int32>(e.StatusCode), e.Message, watch.elapsedMicroseconds());
                /// A final-path-scoped credential fails here, on the staging create, rather than at
                /// publication, so the same guidance is useful.
                throw Exception(
                    ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                    "ADLS Gen2 {} failed for `{}`: HTTP {}: {}{}",
                    what,
                    blob_path,
                    static_cast<int>(e.StatusCode),
                    e.Message,
                    e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden ? authorizationHint() : "");
            }

            LOG_WARNING(log, "ADLS Gen2 {} attempt {} for `{}` failed: HTTP {}: {}. Retrying after {} ms.",
                what, attempt, blob_path, static_cast<int>(e.StatusCode), e.Message, backoff_ms);

            sleepForMilliseconds(backoff_ms);
            backoff_ms *= 2;
        }
    }
    log_event(static_cast<Int32>(ErrorCodes::AZURE_BLOB_STORAGE_ERROR), "retries exhausted", watch.elapsedMicroseconds());
    throw Exception(
        ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
        "ADLS Gen2 {} failed for `{}`",
        what,
        blob_path);
}

void WriteBufferFromAzureDataLakeStorage::runOnceRetryingForbidden(
    const std::function<void()> & op,
    const char * what,
    BlobStorageLogElement::EventType event_type)
{
    auto log_event = [&](Int32 error_code, const String & error_message, size_t elapsed_us)
    {
        if (blob_log)
            blob_log->addEvent(
                event_type,
                /* bucket */ container_for_logging,
                /* remote_path */ blob_path,
                /* local_path */ {},
                /* data_size */ 0,
                /* elapsed_microseconds */ elapsed_us,
                error_code,
                error_message);
    };

    Stopwatch watch;
    size_t backoff_ms = 100;
    for (size_t attempt = 1;; ++attempt)
    {
        try
        {
            op();
            log_event(/*error_code=*/ 0, /*error_message=*/ {}, watch.elapsedMicroseconds());
            return;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            /// A 403 is rejected without changing anything, so repeating it is safe, and
            /// `IDisk::checkAccess` relies on that while Azure is still provisioning access.
            /// Any other failure may have taken effect, so it must not be repeated.
            const bool retryable = write_settings.is_initial_access_check
                && e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden;
            if (!retryable || attempt >= max_unexpected_write_error_retries)
            {
                log_event(static_cast<Int32>(e.StatusCode), e.Message, watch.elapsedMicroseconds());
                throw;
            }

            LOG_WARNING(log, "ADLS Gen2 {} attempt {} for `{}` failed: HTTP {}: {}. Retrying after {} ms.",
                what, attempt, blob_path, static_cast<int>(e.StatusCode), e.Message, backoff_ms);

            sleepForMilliseconds(backoff_ms);
            backoff_ms *= 2;
        }
    }
}

void WriteBufferFromAzureDataLakeStorage::ensureCreated()
{
    if (file_created)
        return;

    /// Access conditions belong to the publishing rename, which is the only request that touches the
    /// final path; attaching them here would be vacuous, because staging is always a fresh object.
    Azure::Storage::Files::DataLake::CreateFileOptions create_options;

    for (size_t attempt = 1; !file_created; ++attempt)
    {
        staging_path = makeAdlsGen2StagingPath(prefixed_blob_path, getRandomASCIIString(STAGING_SUFFIX_LENGTH));
        if (staging_path.empty())
            throw Exception(
                ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                "Cannot stage a write to ADLS Gen2 file `{}`: the object key is too long to append a "
                "staging suffix within the {}-byte limit. Staging is a sibling of the target, so only "
                "the file name can be shortened: a shorter parent directory is what makes room",
                blob_path, MAX_BLOB_KEY_SIZE);
        buildStagingClients();

        LOG_TRACE(log, "Entering Create for ADLS Gen2 staging file `{}` (target `{}`)", staging_path, blob_path);
        bool created = false;
        runWithRetries(
            [&]()
            {
                /// Never a plain Create: the name is unreserved, so a user object could occupy it
                /// (StorageObjectStorageSink::validateKey reserves nothing).
                auto response = file_client->CreateIfNotExists(create_options);
                created = response.Value.Created;
                if (created)
                    staging_etag = response.Value.ETag;
            },
            "Create",
            BlobStorageLogElement::EventType::MultiPartUploadCreate,
            /*data_size=*/ 0);

        if (created)
        {
            file_created = true;
            break;
        }

        /// The name is taken. It may also be an object we created whose response was lost and whose
        /// retry the service answered with PathAlreadyExists; either way we must not touch it, and
        /// in the latter case it is left behind because its ETag is unknown to us.
        LOG_WARNING(log, "ADLS Gen2 staging file `{}` already exists, using a different name. "
                    "If this write created it and lost the response, that object is left behind.", staging_path);

        if (attempt >= MAX_STAGING_NAME_ATTEMPTS)
            throw Exception(
                ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                "Cannot stage a write to ADLS Gen2 file `{}`: {} generated staging names were all taken",
                blob_path, attempt);
    }

    LOG_DEBUG(log, "Created ADLS Gen2 staging file `{}` for `{}`", staging_path, blob_path);
}

void WriteBufferFromAzureDataLakeStorage::flushStaging()
{
    Azure::Storage::Files::DataLake::FlushFileOptions flush_options;
    /// Only ever flush the object this write created.
    flush_options.AccessConditions.IfMatch = staging_etag;

    LOG_TRACE(log, "Entering Flush for ADLS Gen2 staging file `{}`", staging_path);
    try
    {
        /// Flush changes the ETag, so a repeat under the same condition can only fail; single-shot it
        /// and let an ambiguous outcome surface as an error.
        runOnceRetryingForbidden(
            [&]()
            {
                auto response = flush_client->Flush(bytes_appended, flush_options);
                staging_etag = response.Value.ETag;
            },
            "Flush",
            BlobStorageLogElement::EventType::MultiPartUploadComplete);
    }
    catch (const Azure::Core::RequestFailedException & e)
    {
        /// This runs before any rename, so the target provably still holds its previous complete
        /// object. Reporting the commit status as unknown here would be wrong, and alarming.
        if (isAmbiguousAzureStatus(e.StatusCode))
            LOG_WARNING(log, "Outcome of the flush of ADLS Gen2 staging file `{}` is unknown, so it may be "
                        "left behind with contents this write cannot identify.", staging_path);

        throw Exception(
            ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
            "ADLS Gen2 Flush failed for `{}`: HTTP {}: {}{}",
            staging_path, static_cast<int>(e.StatusCode), e.Message,
            e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden ? authorizationHint() : "");
    }
}

void WriteBufferFromAzureDataLakeStorage::publish()
{
    Azure::Storage::Files::DataLake::RenameFileOptions rename_options;
    rename_options.DestinationFileSystem = publish_filesystem_path;
    /// The caller's conditional-write semantics apply to the target, which only this request touches.
    if (!write_settings.object_storage_write_if_none_match.empty())
        rename_options.AccessConditions.IfNoneMatch = Azure::ETag(write_settings.object_storage_write_if_none_match);
    if (!write_settings.object_storage_write_if_match.empty())
        rename_options.AccessConditions.IfMatch = Azure::ETag(write_settings.object_storage_write_if_match);
    /// Publish exactly the object that was just flushed, not whatever happens to sit at that path.
    rename_options.SourceAccessConditions.IfMatch = staging_etag;

    LOG_TRACE(log, "Publishing ADLS Gen2 file `{}` from staging `{}`", blob_path, staging_path);
    try
    {
        runOnceRetryingForbidden(
            [&]() { publish_client.RenameFile(staging_path, prefixed_blob_path, rename_options); },
            "Rename",
            BlobStorageLogElement::EventType::MultiPartUploadComplete);
    }
    catch (const Azure::Core::RequestFailedException & e)
    {
        if (isAmbiguousAzureStatus(e.StatusCode))
        {
            /// A rename is a move, so it cannot be retried: the source is gone once it succeeds. The
            /// destination is written by this single request, so whatever happened it holds one
            /// complete object.
            publish_outcome_unknown = true;
            throw Exception(
                ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
                "ADLS Gen2 commit status unknown for `{}`: publishing failed with HTTP {}: {}. The write "
                "may or may not have been published; the file holds either its previous contents or the "
                "new ones, never a partial object. Read it back to see which, then retry if needed.",
                blob_path, static_cast<int>(e.StatusCode), e.Message);
        }

        /// Definite failures, notably a precondition failure (which conditional writers must keep
        /// seeing) and authorization, are reported as themselves.
        throw Exception(
            ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
            "ADLS Gen2 Rename failed for `{}`: HTTP {}: {}{}",
            blob_path, static_cast<int>(e.StatusCode), e.Message,
            e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden ? authorizationHint() : "");
    }

    published = true;
    LOG_DEBUG(log, "Published ADLS Gen2 file `{}` ({} bytes)", blob_path, bytes_appended);
}

void WriteBufferFromAzureDataLakeStorage::cleanupStaging() noexcept
{
    if (!file_created || published)
        return;

    try
    {
        LOG_INFO(log, "Deleting staging ADLS Gen2 file `{}` for unpublished write to `{}`", staging_path, blob_path);
        Azure::Storage::Files::DataLake::DeleteFileOptions delete_options;
        /// Only delete the object this write owns: after an ambiguous rename the name may already
        /// hold someone else's object, and deleting that would be the very bug being fixed here.
        delete_options.AccessConditions.IfMatch = staging_etag;
        file_client->DeleteIfExists(delete_options);
        file_created = false;
    }
    catch (const Azure::Core::RequestFailedException & e)
    {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::PreconditionFailed)
        {
            LOG_WARNING(log, "Not deleting staging ADLS Gen2 file `{}`: it no longer holds the object "
                        "written here. Leaving it in place.", staging_path);
            file_created = false;
            return;
        }
        /// `file_created` is left set, so the destructor gets one more attempt at removing it.
        tryLogCurrentException(log, fmt::format("Failed to delete staging ADLS Gen2 file `{}`", staging_path));
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to delete staging ADLS Gen2 file `{}`", staging_path));
    }
}

void WriteBufferFromAzureDataLakeStorage::appendBufferedData()
{
    const size_t to_append = offset();
    if (to_append == 0)
        return;

    ensureCreated();

    const auto * data_ptr = reinterpret_cast<const uint8_t *>(working_buffer.begin());
    const int64_t offset_for_append = bytes_appended;

    ProfileEvents::increment(ProfileEvents::AzureUpload);

    LOG_TRACE(log, "Entering Append for `{}`: offset={}, len={}", staging_path, offset_for_append, to_append);
    /// No access condition: Append does not change the ETag, and a substituted object would be caught
    /// by the guarded flush, which is the only commit point.
    runWithRetries(
        [&]()
        {
            Azure::Core::IO::MemoryBodyStream stream(data_ptr, to_append);
            file_client->Append(stream, offset_for_append);
        },
        "Append",
        BlobStorageLogElement::EventType::MultiPartUploadWrite,
        to_append);

    bytes_appended += static_cast<int64_t>(to_append);
    LOG_TRACE(log, "Appended for `{}`: bytes_appended={}", staging_path, bytes_appended);
}

void WriteBufferFromAzureDataLakeStorage::nextImpl()
{
    if (is_prefinalized)
        throw Exception(
            ErrorCodes::AZURE_BLOB_STORAGE_ERROR,
            "Cannot write to prefinalized buffer for ADLS Gen2, the file `{}` has already been flushed",
            blob_path);

    appendBufferedData();
}

void WriteBufferFromAzureDataLakeStorage::preFinalize()
{
    if (is_prefinalized)
        return;

    LOG_DEBUG(log, "Entering preFinalize for ADLS Gen2 file `{}`", blob_path);
    next();

    is_prefinalized = true;
    WriteBuffer::set(fake_buffer_when_prefinalized, sizeof(fake_buffer_when_prefinalized));
    /// A write with no data still has to replace the target, so staging is created here even when
    /// `appendBufferedData` never was.
    ensureCreated();
    flushStaging();
    LOG_DEBUG(log, "Flushed ADLS Gen2 staging file `{}` ({} bytes)", staging_path, bytes_appended);
    /// Publishes before `finalized` is set, so a failure still reaches `cancelImpl`.
    publish();
}

void WriteBufferFromAzureDataLakeStorage::finalizeImpl()
{
    if (!is_prefinalized)
        preFinalize();
}

void WriteBufferFromAzureDataLakeStorage::cancelImpl() noexcept
{
    WriteBufferFromFileBase::cancelImpl();

    /// Only the staging object is removed. The target is not ours to delete: this buffer never
    /// created it, and it may hold rows committed by an earlier write.
    cleanupStaging();
}

}

#endif
