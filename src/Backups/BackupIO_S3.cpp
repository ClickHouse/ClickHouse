#include <Backups/BackupIO_S3.h>

#if USE_AWS_S3
#include <Common/quoteString.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Interpreters/Context.h>
#include <Storages/StorageS3Settings.h>
#include <IO/IOThreadPool.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <filesystem>

#include <aws/s3/model/ListObjectsRequest.h>


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
    std::shared_ptr<Aws::S3::S3Client>
    makeS3Client(const S3::URI & s3_uri, const String & access_key_id, const String & secret_access_key, const ContextPtr & context)
    {
        auto settings = context->getStorageS3Settings().getSettings(s3_uri.uri.toString());

        Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key);
        HeaderCollection headers;
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

    Aws::Vector<Aws::S3::Model::Object> listObjects(Aws::S3::S3Client & client, const S3::URI & s3_uri, const String & file_name)
    {
        Aws::S3::Model::ListObjectsRequest request;
        request.SetBucket(s3_uri.bucket);
        request.SetPrefix(fs::path{s3_uri.key} / file_name);
        request.SetMaxKeys(1);
        auto outcome = client.ListObjects(request);
        if (!outcome.IsSuccess())
            throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
        return outcome.GetResult().GetContents();
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
    request_settings.updateFromSettingsIfEmpty(context_->getSettingsRef());
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


void BackupWriterS3::copyObjectImpl(
    const String & src_bucket,
    const String & src_key,
    const String & dst_bucket,
    const String & dst_key,
    const Aws::S3::Model::HeadObjectResult & head,
    const std::optional<ObjectAttributes> & metadata) const
{
    size_t size = head.GetContentLength();
    LOG_TRACE(log, "Copying {} bytes using single-operation copy", size);

    Aws::S3::Model::CopyObjectRequest request;
    request.SetCopySource(src_bucket + "/" + src_key);
    request.SetBucket(dst_bucket);
    request.SetKey(dst_key);
    if (metadata)
    {
        request.SetMetadata(*metadata);
        request.SetMetadataDirective(Aws::S3::Model::MetadataDirective::REPLACE);
    }

    auto outcome = client->CopyObject(request);

    if (!outcome.IsSuccess() && outcome.GetError().GetExceptionName() == "EntityTooLarge")
    { // Can't come here with MinIO, MinIO allows single part upload for large objects.
        copyObjectMultipartImpl(src_bucket, src_key, dst_bucket, dst_key, head, metadata);
        return;
    }

    if (!outcome.IsSuccess())
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);

}

Aws::S3::Model::HeadObjectOutcome BackupWriterS3::requestObjectHeadData(const std::string & bucket_from, const std::string & key) const
{
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucket_from);
    request.SetKey(key);

    return client->HeadObject(request);
}

void BackupWriterS3::copyObjectMultipartImpl(
    const String & src_bucket,
    const String & src_key,
    const String & dst_bucket,
    const String & dst_key,
    const Aws::S3::Model::HeadObjectResult & head,
    const std::optional<ObjectAttributes> & metadata) const
{
    size_t size = head.GetContentLength();
    LOG_TRACE(log, "Copying {} bytes using multipart upload copy", size);

    String multipart_upload_id;

    {
        Aws::S3::Model::CreateMultipartUploadRequest request;
        request.SetBucket(dst_bucket);
        request.SetKey(dst_key);
        if (metadata)
            request.SetMetadata(*metadata);

        auto outcome = client->CreateMultipartUpload(request);

        if (!outcome.IsSuccess())
            throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);

        multipart_upload_id = outcome.GetResult().GetUploadId();
    }

    std::vector<String> part_tags;

    size_t position = 0;
    size_t upload_part_size = request_settings.min_upload_part_size;

    for (size_t part_number = 1; position < size; ++part_number)
    {
        size_t next_position = std::min(position + upload_part_size, size);

        Aws::S3::Model::UploadPartCopyRequest part_request;
        part_request.SetCopySource(src_bucket + "/" + src_key);
        part_request.SetBucket(dst_bucket);
        part_request.SetKey(dst_key);
        part_request.SetUploadId(multipart_upload_id);
        part_request.SetPartNumber(static_cast<int>(part_number));
        part_request.SetCopySourceRange(fmt::format("bytes={}-{}", position, next_position - 1));

        auto outcome = client->UploadPartCopy(part_request);
        if (!outcome.IsSuccess())
        {
            Aws::S3::Model::AbortMultipartUploadRequest abort_request;
            abort_request.SetBucket(dst_bucket);
            abort_request.SetKey(dst_key);
            abort_request.SetUploadId(multipart_upload_id);
            client->AbortMultipartUpload(abort_request);
            // In error case we throw exception later with first error from UploadPartCopy
        }
        if (!outcome.IsSuccess())
            throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);

        auto etag = outcome.GetResult().GetCopyPartResult().GetETag();
        part_tags.push_back(etag);

        position = next_position;

        if (part_number % request_settings.upload_part_size_multiply_parts_count_threshold == 0)
        {
            upload_part_size *= request_settings.upload_part_size_multiply_factor;
            upload_part_size = std::min(upload_part_size, request_settings.max_upload_part_size);
        }
    }

    {
        Aws::S3::Model::CompleteMultipartUploadRequest req;
        req.SetBucket(dst_bucket);
        req.SetKey(dst_key);
        req.SetUploadId(multipart_upload_id);

        Aws::S3::Model::CompletedMultipartUpload multipart_upload;
        for (size_t i = 0; i < part_tags.size(); ++i)
        {
            Aws::S3::Model::CompletedPart part;
            multipart_upload.AddParts(part.WithETag(part_tags[i]).WithPartNumber(static_cast<int>(i) + 1));
        }

        req.SetMultipartUpload(multipart_upload);

        auto outcome = client->CompleteMultipartUpload(req);

        if (!outcome.IsSuccess())
            throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
    }
}

void BackupWriterS3::copyFileNative(DiskPtr from_disk, const String & file_name_from, const String & file_name_to)
{
    if (!from_disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot natively copy data to disk without source disk");

    auto objects = from_disk->getStorageObjects(file_name_from);
    if (objects.size() > 1)
    {
        copyFileThroughBuffer(from_disk->readFile(file_name_from), file_name_to);
    }
    else
    {
        auto object_storage = from_disk->getObjectStorage();
        std::string source_bucket = object_storage->getObjectsNamespace();
        auto file_path = fs::path(s3_uri.key) / file_name_to;

        auto head = requestObjectHeadData(source_bucket, objects[0].absolute_path).GetResult();
        if (static_cast<size_t>(head.GetContentLength()) < request_settings.max_single_operation_copy_size)
        {
            copyObjectImpl(
                source_bucket, objects[0].absolute_path, s3_uri.bucket, file_path, head);
        }
        else
        {
            copyObjectMultipartImpl(
                source_bucket, objects[0].absolute_path, s3_uri.bucket, file_path, head);
        }
    }
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

void BackupWriterS3::removeFiles(const Strings & file_names)
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
        Aws::S3::Model::DeleteObjectsRequest request;
        request.SetBucket(s3_uri.bucket);
        request.SetDelete(delkeys);

        auto outcome = client->DeleteObjects(request);
        if (!outcome.IsSuccess())
            throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
    }
}

}

#endif
