#include <Backups/BackupIO_S3.h>

#if USE_AWS_S3
#include <Common/quoteString.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Interpreters/Context.h>
#include <IO/IOThreadPool.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/copyS3File.h>
#include <IO/S3/Client.h>

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
            /* for_disk_s3 = */ false, /* get_request_throttler = */ {}, /* put_request_throttler = */ {});

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
            std::move(headers),
            settings.auth_settings.use_environment_credentials.value_or(
                context->getConfigRef().getBool("s3.use_environment_credentials", false)),
            settings.auth_settings.use_insecure_imds_request.value_or(
                context->getConfigRef().getBool("s3.use_insecure_imds_request", false)));
    }

    Aws::Vector<Aws::S3::Model::Object> listObjects(S3::Client & client, const S3::URI & s3_uri, const String & file_name)
    {
        S3::ListObjectsRequest request;
        request.SetBucket(s3_uri.bucket);
        request.SetPrefix(fs::path{s3_uri.key} / file_name);
        request.SetMaxKeys(1);
        auto outcome = client.ListObjects(request);
        if (!outcome.IsSuccess())
            throw Exception::createDeprecated(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
        return outcome.GetResult().GetContents();
    }

    bool isNotFoundError(Aws::S3::S3Errors error)
    {
        return error == Aws::S3::S3Errors::RESOURCE_NOT_FOUND
            || error == Aws::S3::S3Errors::NO_SUCH_KEY;
    }
}


BackupReaderS3::BackupReaderS3(
    const S3::URI & s3_uri_, const String & access_key_id_, const String & secret_access_key_, const ContextPtr & context_)
    : s3_uri(s3_uri_)
    , client(makeS3Client(s3_uri_, access_key_id_, secret_access_key_, context_))
    , read_settings(context_->getReadSettings())
    , request_settings(context_->getStorageS3Settings().getSettings(s3_uri.uri.toString()).request_settings)
{
    request_settings.max_single_read_retries = context_->getSettingsRef().s3_max_single_read_retries; // FIXME: Avoid taking value for endpoint
}

DataSourceDescription BackupReaderS3::getDataSourceDescription() const
{
    return DataSourceDescription{DataSourceType::S3, s3_uri.endpoint, false, false};
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


BackupWriterS3::BackupWriterS3(
    const S3::URI & s3_uri_, const String & access_key_id_, const String & secret_access_key_, const ContextPtr & context_)
    : s3_uri(s3_uri_)
    , client(makeS3Client(s3_uri_, access_key_id_, secret_access_key_, context_))
    , read_settings(context_->getReadSettings())
    , request_settings(context_->getStorageS3Settings().getSettings(s3_uri.uri.toString()).request_settings)
    , log(&Poco::Logger::get("BackupWriterS3"))
{
    request_settings.updateFromSettings(context_->getSettingsRef());
    request_settings.max_single_read_retries = context_->getSettingsRef().s3_max_single_read_retries; // FIXME: Avoid taking value for endpoint
}

DataSourceDescription BackupWriterS3::getDataSourceDescription() const
{
    return DataSourceDescription{DataSourceType::S3, s3_uri.endpoint, false, false};
}

bool BackupWriterS3::supportNativeCopy(DataSourceDescription data_source_description) const
{
    return getDataSourceDescription() == data_source_description;
}

void BackupWriterS3::copyFileNative(DiskPtr src_disk, const String & src_file_name, UInt64 src_offset, UInt64 src_size, const String & dest_file_name)
{
    if (!src_disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot natively copy data to disk without source disk");

    auto objects = src_disk->getStorageObjects(src_file_name);
    if (objects.size() > 1)
    {
        auto create_read_buffer = [src_disk, src_file_name] { return src_disk->readFile(src_file_name); };
        copyDataToFile(create_read_buffer, src_offset, src_size, dest_file_name);
    }
    else
    {
        auto object_storage = src_disk->getObjectStorage();
        std::string src_bucket = object_storage->getObjectsNamespace();
        auto file_path = fs::path(s3_uri.key) / dest_file_name;
        copyS3File(client, src_bucket, objects[0].absolute_path, src_offset, src_size, s3_uri.bucket, file_path, request_settings, {},
                   threadPoolCallbackRunner<void>(IOThreadPool::get(), "BackupWriterS3"));
    }
}

void BackupWriterS3::copyDataToFile(
    const CreateReadBufferFunction & create_read_buffer, UInt64 offset, UInt64 size, const String & dest_file_name)
{
    copyDataToS3File(create_read_buffer, offset, size, client, s3_uri.bucket, fs::path(s3_uri.key) / dest_file_name, request_settings, {},
                     threadPoolCallbackRunner<void>(IOThreadPool::get(), "BackupWriterS3"));
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

bool BackupWriterS3::fileContentsEqual(const String & file_name, const String & expected_file_contents)
{
    if (listObjects(*client, s3_uri, file_name).empty())
        return false;

    try
    {
        auto in = std::make_unique<ReadBufferFromS3>(
            client, s3_uri.bucket, fs::path(s3_uri.key) / file_name, s3_uri.version_id, request_settings, read_settings);
        String actual_file_contents(expected_file_contents.size(), ' ');
        return (in->read(actual_file_contents.data(), actual_file_contents.size()) == actual_file_contents.size())
            && (actual_file_contents == expected_file_contents) && in->eof();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }
}

std::unique_ptr<WriteBuffer> BackupWriterS3::writeFile(const String & file_name)
{
    return std::make_unique<WriteBufferFromS3>(
        client,
        s3_uri.bucket,
        fs::path(s3_uri.key) / file_name,
        request_settings,
        std::nullopt,
        DBMS_DEFAULT_BUFFER_SIZE,
        threadPoolCallbackRunner<void>(IOThreadPool::get(), "BackupWriterS3"));
}

void BackupWriterS3::removeFile(const String & file_name)
{
    S3::DeleteObjectRequest request;
    request.SetBucket(s3_uri.bucket);
    request.SetKey(fs::path(s3_uri.key) / file_name);
    auto outcome = client->DeleteObject(request);
    if (!outcome.IsSuccess() && !isNotFoundError(outcome.GetError().GetErrorType()))
        throw Exception::createDeprecated(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
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
            throw Exception::createDeprecated(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
    }
}

}

#endif
