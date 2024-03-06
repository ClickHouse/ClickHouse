#include <Backups/BackupIO_S3.h>

#if USE_AWS_S3
#include <Common/quoteString.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Interpreters/Context.h>
#include <IO/SharedThreadPools.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/copyS3File.h>
#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <Disks/IDisk.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <aws/core/auth/AWSCredentials.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace
{
    std::shared_ptr<S3::Client>
    makeS3Client(const S3::URI & s3_uri, const String & access_key_id, const String & secret_access_key, const ContextPtr & context)
    {
        auto settings = context->getStorageS3Settings().getSettings(s3_uri.uri.toString());

        Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key);
        HTTPHeaderEntries headers;
        if (access_key_id.empty())
        {
            credentials = Aws::Auth::AWSCredentials(settings.auth_settings.access_key_id, settings.auth_settings.secret_access_key);
            headers = settings.auth_settings.headers;
        }

        S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
            settings.auth_settings.region,
            context->getRemoteHostFilter(),
            static_cast<unsigned>(context->getGlobalContext()->getSettingsRef().s3_max_redirects),
            context->getGlobalContext()->getSettingsRef().enable_s3_requests_logging,
            /* for_disk_s3 = */ false, settings.request_settings.get_request_throttler, settings.request_settings.put_request_throttler,
            s3_uri.uri.getScheme());

        client_configuration.endpointOverride = s3_uri.endpoint;
        client_configuration.maxConnections = static_cast<unsigned>(context->getSettingsRef().s3_max_connections);
        /// Increase connect timeout
        client_configuration.connectTimeoutMs = 10 * 1000;
        /// Requests in backups can be extremely long, set to one hour
        client_configuration.requestTimeoutMs = 60 * 60 * 1000;

        return S3::ClientFactory::instance().create(
            client_configuration,
            s3_uri.is_virtual_hosted_style,
            credentials.GetAWSAccessKeyId(),
            credentials.GetAWSSecretKey(),
            settings.auth_settings.server_side_encryption_customer_key_base64,
            settings.auth_settings.server_side_encryption_kms_config,
            std::move(headers),
            S3::CredentialsConfiguration
            {
                settings.auth_settings.use_environment_credentials.value_or(
                    context->getConfigRef().getBool("s3.use_environment_credentials", true)),
                settings.auth_settings.use_insecure_imds_request.value_or(
                    context->getConfigRef().getBool("s3.use_insecure_imds_request", false)),
                settings.auth_settings.expiration_window_seconds.value_or(
                    context->getConfigRef().getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
                settings.auth_settings.no_sign_request.value_or(
                    context->getConfigRef().getBool("s3.no_sign_request", false)),
            });
    }

    Aws::Vector<Aws::S3::Model::Object> listObjects(S3::Client & client, const S3::URI & s3_uri, const String & file_name)
    {
        S3::ListObjectsRequest request;
        request.SetBucket(s3_uri.bucket);
        request.SetPrefix(fs::path{s3_uri.key} / file_name);
        request.SetMaxKeys(1);
        auto outcome = client.ListObjects(request);
        if (!outcome.IsSuccess())
            throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
        return outcome.GetResult().GetContents();
    }

    bool isNotFoundError(Aws::S3::S3Errors error)
    {
        return error == Aws::S3::S3Errors::RESOURCE_NOT_FOUND
            || error == Aws::S3::S3Errors::NO_SUCH_KEY;
    }
}


BackupReaderS3::BackupReaderS3(
    const S3::URI & s3_uri_,
    const String & access_key_id_,
    const String & secret_access_key_,
    bool allow_s3_native_copy,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const ContextPtr & context_)
    : BackupReaderDefault(read_settings_, write_settings_, &Poco::Logger::get("BackupReaderS3"))
    , s3_uri(s3_uri_)
    , client(makeS3Client(s3_uri_, access_key_id_, secret_access_key_, context_))
    , request_settings(context_->getStorageS3Settings().getSettings(s3_uri.uri.toString()).request_settings)
    , data_source_description{DataSourceType::S3, s3_uri.endpoint, false, false}
{
    request_settings.updateFromSettings(context_->getSettingsRef());
    request_settings.max_single_read_retries = context_->getSettingsRef().s3_max_single_read_retries; // FIXME: Avoid taking value for endpoint
    request_settings.allow_native_copy = allow_s3_native_copy;
}

BackupReaderS3::~BackupReaderS3() = default;

bool BackupReaderS3::fileExists(const String & file_name)
{
    return !listObjects(*client, s3_uri, file_name).empty();
}

UInt64 BackupReaderS3::getFileSize(const String & file_name)
{
    auto objects = listObjects(*client, s3_uri, file_name);
    if (objects.empty())
        throw Exception(ErrorCodes::S3_ERROR, "Object {} must exist");
    return objects[0].GetSize();
}

std::unique_ptr<SeekableReadBuffer> BackupReaderS3::readFile(const String & file_name)
{
    return std::make_unique<ReadBufferFromS3>(
        client, s3_uri.bucket, fs::path(s3_uri.key) / file_name, s3_uri.version_id, request_settings, read_settings);
}

void BackupReaderS3::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                    DiskPtr destination_disk, const String & destination_path, WriteMode write_mode)
{
    /// Use the native copy as a more optimal way to copy a file from S3 to S3 if it's possible.
    /// We don't check for `has_throttling` here because the native copy almost doesn't use network.
    auto destination_data_source_description = destination_disk->getDataSourceDescription();
    if (destination_data_source_description.sameKind(data_source_description)
        && (destination_data_source_description.is_encrypted == encrypted_in_backup))
    {
        LOG_TRACE(log, "Copying {} from S3 to disk {}", path_in_backup, destination_disk->getName());
        auto write_blob_function = [&](const Strings & blob_path, WriteMode mode, const std::optional<ObjectAttributes> & object_attributes) -> size_t
        {
            /// Object storage always uses mode `Rewrite` because it simulates append using metadata and different files.
            if (blob_path.size() != 2 || mode != WriteMode::Rewrite)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Blob writing function called with unexpected blob_path.size={} or mode={}",
                                blob_path.size(), mode);

            copyS3File(
                client,
                client,
                s3_uri.bucket,
                fs::path(s3_uri.key) / path_in_backup,
                0,
                file_size,
                /* dest_bucket= */ blob_path[1],
                /* dest_key= */ blob_path[0],
                request_settings,
                object_attributes,
                threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupReaderS3"),
                /* for_disk_s3= */ true);

            return file_size;
        };

        destination_disk->writeFileUsingBlobWritingFunction(destination_path, write_mode, write_blob_function);
        return; /// copied!
    }

    /// Fallback to copy through buffers.
    BackupReaderDefault::copyFileToDisk(path_in_backup, file_size, encrypted_in_backup, destination_disk, destination_path, write_mode);
}


BackupWriterS3::BackupWriterS3(
    const S3::URI & s3_uri_,
    const String & access_key_id_,
    const String & secret_access_key_,
    bool allow_s3_native_copy,
    const String & storage_class_name,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const ContextPtr & context_)
    : BackupWriterDefault(read_settings_, write_settings_, &Poco::Logger::get("BackupWriterS3"))
    , s3_uri(s3_uri_)
    , client(makeS3Client(s3_uri_, access_key_id_, secret_access_key_, context_))
    , request_settings(context_->getStorageS3Settings().getSettings(s3_uri.uri.toString()).request_settings)
    , data_source_description{DataSourceType::S3, s3_uri.endpoint, false, false}
{
    request_settings.updateFromSettings(context_->getSettingsRef());
    request_settings.max_single_read_retries = context_->getSettingsRef().s3_max_single_read_retries; // FIXME: Avoid taking value for endpoint
    request_settings.allow_native_copy = allow_s3_native_copy;
    request_settings.setStorageClassName(storage_class_name);
}

void BackupWriterS3::copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                                      bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    /// Use the native copy as a more optimal way to copy a file from S3 to S3 if it's possible.
    /// We don't check for `has_throttling` here because the native copy almost doesn't use network.
    auto source_data_source_description = src_disk->getDataSourceDescription();
    if (source_data_source_description.sameKind(data_source_description) && (source_data_source_description.is_encrypted == copy_encrypted))
    {
        /// getBlobPath() can return more than 3 elements if the file is stored as multiple objects in S3 bucket.
        /// In this case we can't use the native copy.
        if (auto blob_path = src_disk->getBlobPath(src_path); blob_path.size() == 2)
        {
            LOG_TRACE(log, "Copying file {} from disk {} to S3", src_path, src_disk->getName());
            copyS3File(
                client,
                client,
                /* src_bucket */ blob_path[1],
                /* src_key= */ blob_path[0],
                start_pos,
                length,
                s3_uri.bucket,
                fs::path(s3_uri.key) / path_in_backup,
                request_settings,
                {},
                threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupWriterS3"));
            return; /// copied!
        }
    }

    /// Fallback to copy through buffers.
    BackupWriterDefault::copyFileFromDisk(path_in_backup, src_disk, src_path, copy_encrypted, start_pos, length);
}

void BackupWriterS3::copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length)
{
    copyDataToS3File(create_read_buffer, start_pos, length, client, client, s3_uri.bucket, fs::path(s3_uri.key) / path_in_backup, request_settings, {},
                     threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupWriterS3"));
}

BackupWriterS3::~BackupWriterS3() = default;

bool BackupWriterS3::fileExists(const String & file_name)
{
    return !listObjects(*client, s3_uri, file_name).empty();
}

UInt64 BackupWriterS3::getFileSize(const String & file_name)
{
    auto objects = listObjects(*client, s3_uri, file_name);
    if (objects.empty())
        throw Exception(ErrorCodes::S3_ERROR, "Object {} must exist");
    return objects[0].GetSize();
}

std::unique_ptr<ReadBuffer> BackupWriterS3::readFile(const String & file_name, size_t expected_file_size)
{
    return std::make_unique<ReadBufferFromS3>(
            client, s3_uri.bucket, fs::path(s3_uri.key) / file_name, s3_uri.version_id, request_settings, read_settings,
            false, 0, 0, false, expected_file_size);
}

std::unique_ptr<WriteBuffer> BackupWriterS3::writeFile(const String & file_name)
{
    return std::make_unique<WriteBufferFromS3>(
        client,
        client, // already has long timeout
        s3_uri.bucket,
        fs::path(s3_uri.key) / file_name,
        DBMS_DEFAULT_BUFFER_SIZE,
        request_settings,
        std::nullopt,
        threadPoolCallbackRunner<void>(getBackupsIOThreadPool().get(), "BackupWriterS3"),
        write_settings);
}

void BackupWriterS3::removeFile(const String & file_name)
{
    S3::DeleteObjectRequest request;
    request.SetBucket(s3_uri.bucket);
    request.SetKey(fs::path(s3_uri.key) / file_name);
    auto outcome = client->DeleteObject(request);
    if (!outcome.IsSuccess() && !isNotFoundError(outcome.GetError().GetErrorType()))
        throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
}

void BackupWriterS3::removeFiles(const Strings & file_names)
{
    try
    {
        if (!supports_batch_delete.has_value() || supports_batch_delete.value() == true)
        {
            removeFilesBatch(file_names);
            supports_batch_delete = true;
        }
        else
        {
            for (const auto & file_name : file_names)
                removeFile(file_name);
        }
    }
    catch (const Exception &)
    {
        if (!supports_batch_delete.has_value())
        {
            supports_batch_delete = false;
            LOG_TRACE(log, "DeleteObjects is not supported. Retrying with plain DeleteObject.");

            for (const auto & file_name : file_names)
                removeFile(file_name);
        }
        else
            throw;
    }

}

void BackupWriterS3::removeFilesBatch(const Strings & file_names)
{
    /// One call of DeleteObjects() cannot remove more than 1000 keys.
    size_t chunk_size_limit = 1000;

    size_t current_position = 0;
    while (current_position < file_names.size())
    {
        std::vector<Aws::S3::Model::ObjectIdentifier> current_chunk;
        for (; current_position < file_names.size() && current_chunk.size() < chunk_size_limit; ++current_position)
        {
            Aws::S3::Model::ObjectIdentifier obj;
            obj.SetKey(fs::path(s3_uri.key) / file_names[current_position]);
            current_chunk.push_back(obj);
        }

        Aws::S3::Model::Delete delkeys;
        delkeys.SetObjects(current_chunk);
        S3::DeleteObjectsRequest request;
        request.SetBucket(s3_uri.bucket);
        request.SetDelete(delkeys);

        auto outcome = client->DeleteObjects(request);
        if (!outcome.IsSuccess() && !isNotFoundError(outcome.GetError().GetErrorType()))
            throw S3Exception(outcome.GetError().GetMessage(), outcome.GetError().GetErrorType());
    }
}

}

#endif
