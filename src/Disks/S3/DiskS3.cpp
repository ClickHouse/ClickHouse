#include "DiskS3.h"

#if USE_AWS_S3
#include "Disks/DiskFactory.h"

#include <bitset>
#include <random>
#include <utility>

#include <boost/algorithm/string.hpp>

#include <base/unit.h>
#include <base/FnTraits.h>

#include <Common/checkStackSize.h>
#include <Common/createHardLink.h>
#include <Common/quoteString.h>
#include <Common/thread_local_rng.h>

#include <Interpreters/Context.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>

#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartCopyRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int FILE_ALREADY_EXISTS;
    extern const int UNKNOWN_FORMAT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

/// Helper class to collect keys into chunks of maximum size (to prepare batch requests to AWS API)
/// see https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
class S3PathKeeper : public RemoteFSPathKeeper
{
public:
    using Chunk = Aws::Vector<Aws::S3::Model::ObjectIdentifier>;
    using Chunks = std::list<Chunk>;

    explicit S3PathKeeper(size_t chunk_limit_) : RemoteFSPathKeeper(chunk_limit_) {}

    void addPath(const String & path) override
    {
        if (chunks.empty() || chunks.back().size() >= chunk_limit)
        {
            /// add one more chunk
            chunks.push_back(Chunks::value_type());
            chunks.back().reserve(chunk_limit);
        }
        Aws::S3::Model::ObjectIdentifier obj;
        obj.SetKey(path);
        chunks.back().push_back(obj);
    }

    void removePaths(Fn<void(Chunk &&)> auto && remove_chunk_func)
    {
        for (auto & chunk : chunks)
            remove_chunk_func(std::move(chunk));
    }

    static String getChunkKeys(const Chunk & chunk)
    {
        String res;
        for (const auto & obj : chunk)
        {
            const auto & key = obj.GetKey();
            if (!res.empty())
                res.append(", ");
            res.append(key.c_str(), key.size());
        }
        return res;
    }

private:
    Chunks chunks;
};

String getRandomName()
{
    std::uniform_int_distribution<int> distribution('a', 'z');
    String res(32, ' '); /// The number of bits of entropy should be not less than 128.
    for (auto & c : res)
        c = distribution(thread_local_rng);
    return res;
}

template <typename Result, typename Error>
void throwIfError(Aws::Utils::Outcome<Result, Error> & response)
{
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw Exception(std::to_string(static_cast<int>(err.GetErrorType())) + ": " + err.GetMessage(), ErrorCodes::S3_ERROR);
    }
}

template <typename Result, typename Error>
void throwIfError(const Aws::Utils::Outcome<Result, Error> & response)
{
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
    }
}
template <typename Result, typename Error>
void logIfError(Aws::Utils::Outcome<Result, Error> & response, Fn<String()> auto && msg)
{
    try
    {
        throwIfError(response);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, msg());
    }
}

template <typename Result, typename Error>
void logIfError(const Aws::Utils::Outcome<Result, Error> & response, Fn<String()> auto && msg)
{
    try
    {
        throwIfError(response);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, msg());
    }
}

DiskS3::DiskS3(
    String name_,
    String bucket_,
    String s3_root_path_,
    String metadata_path_,
    ContextPtr context_,
    SettingsPtr settings_,
    GetDiskSettings settings_getter_)
    : IDiskRemote(name_, s3_root_path_, metadata_path_, "DiskS3", settings_->thread_pool_size)
    , bucket(std::move(bucket_))
    , current_settings(std::move(settings_))
    , settings_getter(settings_getter_)
    , context(context_)
{
}

RemoteFSPathKeeperPtr DiskS3::createFSPathKeeper() const
{
    auto settings = current_settings.get();
    return std::make_shared<S3PathKeeper>(settings->objects_chunk_size_to_delete);
}

void DiskS3::removeFromRemoteFS(RemoteFSPathKeeperPtr fs_paths_keeper)
{
    auto settings = current_settings.get();
    auto * s3_paths_keeper = dynamic_cast<S3PathKeeper *>(fs_paths_keeper.get());

    if (s3_paths_keeper)
        s3_paths_keeper->removePaths([&](S3PathKeeper::Chunk && chunk)
        {
            String keys = S3PathKeeper::getChunkKeys(chunk);
            LOG_TRACE(log, "Remove AWS keys {}", keys);
            Aws::S3::Model::Delete delkeys;
            delkeys.SetObjects(chunk);
            Aws::S3::Model::DeleteObjectsRequest request;
            request.SetBucket(bucket);
            request.SetDelete(delkeys);
            auto outcome = settings->client->DeleteObjects(request);
            // Do not throw here, continue deleting other chunks
            logIfError(outcome, [&](){return "Can't remove AWS keys: " + keys;});
        });
}

void DiskS3::moveFile(const String & from_path, const String & to_path)
{
    auto settings = current_settings.get();

    moveFile(from_path, to_path, settings->send_metadata);
}

void DiskS3::moveFile(const String & from_path, const String & to_path, bool send_metadata)
{
    if (exists(to_path))
        throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);

    if (send_metadata)
    {
        auto revision = ++revision_counter;
        const ObjectMetadata object_metadata {
            {"from_path", from_path},
            {"to_path", to_path}
        };
        createFileOperationObject("rename", revision, object_metadata);
    }

    fs::rename(fs::path(metadata_path) / from_path, fs::path(metadata_path) / to_path);
}

std::unique_ptr<ReadBufferFromFileBase> DiskS3::readFile(const String & path, const ReadSettings & read_settings, std::optional<size_t>) const
{
    auto settings = current_settings.get();
    auto metadata = readMeta(path);

    LOG_TRACE(log, "Read from file by path: {}. Existing S3 objects: {}",
        backQuote(metadata_path + path), metadata.remote_fs_objects.size());

    bool threadpool_read = read_settings.remote_fs_method == RemoteFSReadMethod::read_threadpool;

    auto s3_impl = std::make_unique<ReadBufferFromS3Gather>(
        path,
        settings->client, bucket, metadata,
        settings->s3_max_single_read_retries, read_settings, threadpool_read);

    if (threadpool_read)
    {
        auto reader = getThreadPoolReader();
        return std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(reader, read_settings, std::move(s3_impl));
    }
    else
    {
        auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(s3_impl));
        return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), settings->min_bytes_for_seek);
    }
}

std::unique_ptr<WriteBufferFromFileBase> DiskS3::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    auto settings = current_settings.get();
    auto metadata = readOrCreateMetaForWriting(path, mode);

    /// Path to store new S3 object.
    auto s3_path = getRandomName();

    std::optional<ObjectMetadata> object_metadata;
    if (settings->send_metadata)
    {
        auto revision = ++revision_counter;
        object_metadata = {
            {"path", path}
        };
        s3_path = "r" + revisionToString(revision) + "-file-" + s3_path;
    }

    LOG_TRACE(log, "{} to file by path: {}. S3 path: {}",
              mode == WriteMode::Rewrite ? "Write" : "Append", backQuote(metadata_path + path), remote_fs_root_path + s3_path);

    auto s3_buffer = std::make_unique<WriteBufferFromS3>(
        settings->client,
        bucket,
        metadata.remote_fs_root_path + s3_path,
        settings->s3_min_upload_part_size,
        settings->s3_max_single_part_upload_size,
        std::move(object_metadata),
        buf_size);

    return std::make_unique<WriteIndirectBufferFromRemoteFS<WriteBufferFromS3>>(std::move(s3_buffer), std::move(metadata), s3_path);
}

void DiskS3::createHardLink(const String & src_path, const String & dst_path)
{
    auto settings = current_settings.get();
    createHardLink(src_path, dst_path, settings->send_metadata);
}

void DiskS3::createHardLink(const String & src_path, const String & dst_path, bool send_metadata)
{
    /// We don't need to record hardlinks created to shadow folder.
    if (send_metadata && !dst_path.starts_with("shadow/"))
    {
        auto revision = ++revision_counter;
        const ObjectMetadata object_metadata {
            {"src_path", src_path},
            {"dst_path", dst_path}
        };
        createFileOperationObject("hardlink", revision, object_metadata);
    }

    /// Increment number of references.
    auto src = readMeta(src_path);
    ++src.ref_count;
    src.save();

    /// Create FS hardlink to metadata file.
    DB::createHardLink(metadata_path + src_path, metadata_path + dst_path);
}

void DiskS3::shutdown()
{
    auto settings = current_settings.get();
    /// This call stops any next retry attempts for ongoing S3 requests.
    /// If S3 request is failed and the method below is executed S3 client immediately returns the last failed S3 request outcome.
    /// If S3 is healthy nothing wrong will be happened and S3 requests will be processed in a regular way without errors.
    /// This should significantly speed up shutdown process if S3 is unhealthy.
    settings->client->DisableRequestProcessing();
}

void DiskS3::createFileOperationObject(const String & operation_name, UInt64 revision, const DiskS3::ObjectMetadata & metadata)
{
    auto settings = current_settings.get();
    const String key = "operations/r" + revisionToString(revision) + "-" + operation_name;
    WriteBufferFromS3 buffer(
        settings->client,
        bucket,
        remote_fs_root_path + key,
        settings->s3_min_upload_part_size,
        settings->s3_max_single_part_upload_size,
        metadata);

    buffer.write('0');
    buffer.finalize();
}

void DiskS3::startup()
{
    auto settings = current_settings.get();

    /// Need to be enabled if it was disabled during shutdown() call.
    settings->client->EnableRequestProcessing();

    if (!settings->send_metadata)
        return;

    LOG_INFO(log, "Starting up disk {}", name);

    restore();

    if (readSchemaVersion(bucket, remote_fs_root_path) < RESTORABLE_SCHEMA_VERSION)
        migrateToRestorableSchema();

    findLastRevision();

    LOG_INFO(log, "Disk {} started up", name);
}

void DiskS3::findLastRevision()
{
    /// Construct revision number from high to low bits.
    String revision;
    revision.reserve(64);
    for (int bit = 0; bit < 64; bit++)
    {
        auto revision_prefix = revision + "1";

        LOG_TRACE(log, "Check object exists with revision prefix {}", revision_prefix);

        /// Check file or operation with such revision prefix exists.
        if (checkObjectExists(bucket, remote_fs_root_path + "r" + revision_prefix)
            || checkObjectExists(bucket, remote_fs_root_path + "operations/r" + revision_prefix))
            revision += "1";
        else
            revision += "0";
    }
    revision_counter = static_cast<UInt64>(std::bitset<64>(revision).to_ullong());
    LOG_INFO(log, "Found last revision number {} for disk {}", revision_counter, name);
}

int DiskS3::readSchemaVersion(const String & source_bucket, const String & source_path)
{
    int version = 0;
    if (!checkObjectExists(source_bucket, source_path + SCHEMA_VERSION_OBJECT))
        return version;

    auto settings = current_settings.get();
    ReadBufferFromS3 buffer(
        settings->client,
        source_bucket,
        source_path + SCHEMA_VERSION_OBJECT,
        settings->s3_max_single_read_retries,
        context->getReadSettings());

    readIntText(version, buffer);

    return version;
}

void DiskS3::saveSchemaVersion(const int & version)
{
    auto settings = current_settings.get();

    WriteBufferFromS3 buffer(
        settings->client,
        bucket,
        remote_fs_root_path + SCHEMA_VERSION_OBJECT,
        settings->s3_min_upload_part_size,
        settings->s3_max_single_part_upload_size);

    writeIntText(version, buffer);
    buffer.finalize();
}

void DiskS3::updateObjectMetadata(const String & key, const ObjectMetadata & metadata)
{
    copyObjectImpl(bucket, key, bucket, key, std::nullopt, metadata);
}

void DiskS3::migrateFileToRestorableSchema(const String & path)
{
    LOG_TRACE(log, "Migrate file {} to restorable schema", metadata_path + path);

    auto meta = readMeta(path);

    for (const auto & [key, _] : meta.remote_fs_objects)
    {
        ObjectMetadata metadata {
            {"path", path}
        };
        updateObjectMetadata(remote_fs_root_path + key, metadata);
    }
}

void DiskS3::migrateToRestorableSchemaRecursive(const String & path, Futures & results)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    LOG_TRACE(log, "Migrate directory {} to restorable schema", metadata_path + path);

    bool dir_contains_only_files = true;
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        if (isDirectory(it->path()))
        {
            dir_contains_only_files = false;
            break;
        }

    /// The whole directory can be migrated asynchronously.
    if (dir_contains_only_files)
    {
        auto result = getExecutor().execute([this, path]
             {
                 for (auto it = iterateDirectory(path); it->isValid(); it->next())
                     migrateFileToRestorableSchema(it->path());
             });

        results.push_back(std::move(result));
    }
    else
    {
        for (auto it = iterateDirectory(path); it->isValid(); it->next())
            if (!isDirectory(it->path()))
            {
                auto source_path = it->path();
                auto result = getExecutor().execute([this, source_path]
                    {
                        migrateFileToRestorableSchema(source_path);
                    });

                results.push_back(std::move(result));
            }
            else
                migrateToRestorableSchemaRecursive(it->path(), results);
    }
}

void DiskS3::migrateToRestorableSchema()
{
    try
    {
        LOG_INFO(log, "Start migration to restorable schema for disk {}", name);

        Futures results;

        for (const auto & root : data_roots)
            if (exists(root))
                migrateToRestorableSchemaRecursive(root + '/', results);

        for (auto & result : results)
            result.wait();
        for (auto & result : results)
            result.get();

        saveSchemaVersion(RESTORABLE_SCHEMA_VERSION);
    }
    catch (const Exception &)
    {
        tryLogCurrentException(log, fmt::format("Failed to migrate to restorable schema for disk {}", name));

        throw;
    }
}

bool DiskS3::checkObjectExists(const String & source_bucket, const String & prefix) const
{
    auto settings = current_settings.get();
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(source_bucket);
    request.SetPrefix(prefix);
    request.SetMaxKeys(1);

    auto outcome = settings->client->ListObjectsV2(request);
    throwIfError(outcome);

    return !outcome.GetResult().GetContents().empty();
}

bool DiskS3::checkUniqueId(const String & id) const
{
    auto settings = current_settings.get();
    /// Check that we have right s3 and have access rights
    /// Actually interprets id as s3 object name and checks if it exists
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(bucket);
    request.SetPrefix(id);

    auto outcome = settings->client->ListObjectsV2(request);
    throwIfError(outcome);

    Aws::Vector<Aws::S3::Model::Object> object_list = outcome.GetResult().GetContents();

    for (const auto & object : object_list)
        if (object.GetKey() == id)
            return true;
    return false;
}

Aws::S3::Model::HeadObjectResult DiskS3::headObject(const String & source_bucket, const String & key) const
{
    auto settings = current_settings.get();
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(source_bucket);
    request.SetKey(key);

    auto outcome = settings->client->HeadObject(request);
    throwIfError(outcome);

    return outcome.GetResultWithOwnership();
}

void DiskS3::listObjects(const String & source_bucket, const String & source_path, std::function<bool(const Aws::S3::Model::ListObjectsV2Result &)> callback) const
{
    auto settings = current_settings.get();
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(source_bucket);
    request.SetPrefix(source_path);
    request.SetMaxKeys(settings->list_object_keys_size);

    Aws::S3::Model::ListObjectsV2Outcome outcome;
    do
    {
        outcome = settings->client->ListObjectsV2(request);
        throwIfError(outcome);

        bool should_continue = callback(outcome.GetResult());

        if (!should_continue)
            break;

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
    } while (outcome.GetResult().GetIsTruncated());
}

void DiskS3::copyObject(const String & src_bucket, const String & src_key, const String & dst_bucket, const String & dst_key,
    std::optional<Aws::S3::Model::HeadObjectResult> head) const
{
    if (head && (head->GetContentLength() >= static_cast<Int64>(5_GiB)))
        copyObjectMultipartImpl(src_bucket, src_key, dst_bucket, dst_key, head);
    else
        copyObjectImpl(src_bucket, src_key, dst_bucket, dst_key);
}

void DiskS3::copyObjectImpl(const String & src_bucket, const String & src_key, const String & dst_bucket, const String & dst_key,
    std::optional<Aws::S3::Model::HeadObjectResult> head,
    std::optional<std::reference_wrapper<const ObjectMetadata>> metadata) const
{
    auto settings = current_settings.get();
    Aws::S3::Model::CopyObjectRequest request;
    request.SetCopySource(src_bucket + "/" + src_key);
    request.SetBucket(dst_bucket);
    request.SetKey(dst_key);
    if (metadata)
    {
        request.SetMetadata(*metadata);
        request.SetMetadataDirective(Aws::S3::Model::MetadataDirective::REPLACE);
    }

    auto outcome = settings->client->CopyObject(request);

    if (!outcome.IsSuccess() && outcome.GetError().GetExceptionName() == "EntityTooLarge")
    { // Can't come here with MinIO, MinIO allows single part upload for large objects.
        copyObjectMultipartImpl(src_bucket, src_key, dst_bucket, dst_key, head, metadata);
        return;
    }

    throwIfError(outcome);
}

void DiskS3::copyObjectMultipartImpl(const String & src_bucket, const String & src_key, const String & dst_bucket, const String & dst_key,
    std::optional<Aws::S3::Model::HeadObjectResult> head,
    std::optional<std::reference_wrapper<const ObjectMetadata>> metadata) const
{
    LOG_TRACE(log, "Multipart copy upload has created. Src Bucket: {}, Src Key: {}, Dst Bucket: {}, Dst Key: {}, Metadata: {}",
        src_bucket, src_key, dst_bucket, dst_key, metadata ? "REPLACE" : "NOT_SET");

    auto settings = current_settings.get();

    if (!head)
        head = headObject(src_bucket, src_key);

    size_t size = head->GetContentLength();

    String multipart_upload_id;

    {
        Aws::S3::Model::CreateMultipartUploadRequest request;
        request.SetBucket(dst_bucket);
        request.SetKey(dst_key);
        if (metadata)
            request.SetMetadata(*metadata);

        auto outcome = settings->client->CreateMultipartUpload(request);

        throwIfError(outcome);

        multipart_upload_id = outcome.GetResult().GetUploadId();
    }

    std::vector<String> part_tags;

    size_t upload_part_size = settings->s3_min_upload_part_size;
    for (size_t position = 0, part_number = 1; position < size; ++part_number, position += upload_part_size)
    {
        Aws::S3::Model::UploadPartCopyRequest part_request;
        part_request.SetCopySource(src_bucket + "/" + src_key);
        part_request.SetBucket(dst_bucket);
        part_request.SetKey(dst_key);
        part_request.SetUploadId(multipart_upload_id);
        part_request.SetPartNumber(part_number);
        part_request.SetCopySourceRange(fmt::format("bytes={}-{}", position, std::min(size, position + upload_part_size) - 1));

        auto outcome = settings->client->UploadPartCopy(part_request);
        if (!outcome.IsSuccess())
        {
            Aws::S3::Model::AbortMultipartUploadRequest abort_request;
            abort_request.SetBucket(dst_bucket);
            abort_request.SetKey(dst_key);
            abort_request.SetUploadId(multipart_upload_id);
            settings->client->AbortMultipartUpload(abort_request);
            // In error case we throw exception later with first error from UploadPartCopy
        }
        throwIfError(outcome);

        auto etag = outcome.GetResult().GetCopyPartResult().GetETag();
        part_tags.push_back(etag);
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
            multipart_upload.AddParts(part.WithETag(part_tags[i]).WithPartNumber(i + 1));
        }

        req.SetMultipartUpload(multipart_upload);

        auto outcome = settings->client->CompleteMultipartUpload(req);

        throwIfError(outcome);

        LOG_TRACE(log, "Multipart copy upload has completed. Src Bucket: {}, Src Key: {}, Dst Bucket: {}, Dst Key: {}, "
            "Upload_id: {}, Parts: {}", src_bucket, src_key, dst_bucket, dst_key, multipart_upload_id, part_tags.size());
    }
}

struct DiskS3::RestoreInformation
{
    UInt64 revision = LATEST_REVISION;
    String source_bucket;
    String source_path;
    bool detached = false;
};

void DiskS3::readRestoreInformation(DiskS3::RestoreInformation & restore_information)
{
    ReadBufferFromFile buffer(metadata_path + RESTORE_FILE_NAME, 512);
    buffer.next();

    try
    {
        std::map<String, String> properties;

        while (buffer.hasPendingData())
        {
            String property;
            readText(property, buffer);
            assertChar('\n', buffer);

            auto pos = property.find('=');
            if (pos == String::npos || pos == 0 || pos == property.length())
                throw Exception(fmt::format("Invalid property {} in restore file", property), ErrorCodes::UNKNOWN_FORMAT);

            auto key = property.substr(0, pos);
            auto value = property.substr(pos + 1);

            auto it = properties.find(key);
            if (it != properties.end())
                throw Exception(fmt::format("Property key duplication {} in restore file", key), ErrorCodes::UNKNOWN_FORMAT);

            properties[key] = value;
        }

        for (const auto & [key, value] : properties)
        {
            ReadBufferFromString value_buffer (value);

            if (key == "revision")
                readIntText(restore_information.revision, value_buffer);
            else if (key == "source_bucket")
                readText(restore_information.source_bucket, value_buffer);
            else if (key == "source_path")
                readText(restore_information.source_path, value_buffer);
            else if (key == "detached")
                readBoolTextWord(restore_information.detached, value_buffer);
            else
                throw Exception(fmt::format("Unknown key {} in restore file", key), ErrorCodes::UNKNOWN_FORMAT);
        }
    }
    catch (const Exception &)
    {
        tryLogCurrentException(log, "Failed to read restore information");
        throw;
    }
}

void DiskS3::restore()
{
    if (!exists(RESTORE_FILE_NAME))
        return;

    try
    {
        RestoreInformation information;
        information.source_bucket = bucket;
        information.source_path = remote_fs_root_path;

        readRestoreInformation(information);
        if (information.revision == 0)
            information.revision = LATEST_REVISION;
        if (!information.source_path.ends_with('/'))
            information.source_path += '/';

        if (information.source_bucket == bucket)
        {
            /// In this case we need to additionally cleanup S3 from objects with later revision.
            /// Will be simply just restore to different path.
            if (information.source_path == remote_fs_root_path && information.revision != LATEST_REVISION)
                throw Exception("Restoring to the same bucket and path is allowed if revision is latest (0)", ErrorCodes::BAD_ARGUMENTS);

            /// This case complicates S3 cleanup in case of unsuccessful restore.
            if (information.source_path != remote_fs_root_path && remote_fs_root_path.starts_with(information.source_path))
                throw Exception("Restoring to the same bucket is allowed only if source path is not a sub-path of configured path in S3 disk", ErrorCodes::BAD_ARGUMENTS);
        }

        LOG_INFO(log, "Starting to restore disk {}. Revision: {}, Source bucket: {}, Source path: {}",
                 name, information.revision, information.source_bucket, information.source_path);

        if (readSchemaVersion(information.source_bucket, information.source_path) < RESTORABLE_SCHEMA_VERSION)
            throw Exception("Source bucket doesn't have restorable schema.", ErrorCodes::BAD_ARGUMENTS);

        LOG_INFO(log, "Removing old metadata...");

        bool cleanup_s3 = information.source_bucket != bucket || information.source_path != remote_fs_root_path;
        for (const auto & root : data_roots)
            if (exists(root))
                removeSharedRecursive(root + '/', !cleanup_s3);

        restoreFiles(information);
        restoreFileOperations(information);

        fs::path restore_file = fs::path(metadata_path) / RESTORE_FILE_NAME;
        fs::remove(restore_file);

        saveSchemaVersion(RESTORABLE_SCHEMA_VERSION);

        LOG_INFO(log, "Restore disk {} finished", name);
    }
    catch (const Exception &)
    {
        tryLogCurrentException(log, fmt::format("Failed to restore disk {}", name));

        throw;
    }
}

void DiskS3::restoreFiles(const RestoreInformation & restore_information)
{
    LOG_INFO(log, "Starting restore files for disk {}", name);

    std::vector<std::future<void>> results;
    auto restore_files = [this, &restore_information, &results](auto list_result)
    {
        std::vector<String> keys;
        for (const auto & row : list_result.GetContents())
        {
            const String & key = row.GetKey();

            /// Skip file operations objects. They will be processed separately.
            if (key.find("/operations/") != String::npos)
                continue;

            const auto [revision, _] = extractRevisionAndOperationFromKey(key);
            /// Filter early if it's possible to get revision from key.
            if (revision > restore_information.revision)
                continue;

            keys.push_back(key);
        }

        if (!keys.empty())
        {
            auto result = getExecutor().execute([this, &restore_information, keys]()
            {
                processRestoreFiles(restore_information.source_bucket, restore_information.source_path, keys);
            });

            results.push_back(std::move(result));
        }

        return true;
    };

    /// Execute.
    listObjects(restore_information.source_bucket, restore_information.source_path, restore_files);

    for (auto & result : results)
        result.wait();
    for (auto & result : results)
        result.get();

    LOG_INFO(log, "Files are restored for disk {}", name);
}

void DiskS3::processRestoreFiles(const String & source_bucket, const String & source_path, Strings keys)
{
    for (const auto & key : keys)
    {
        auto head_result = headObject(source_bucket, key);
        auto object_metadata = head_result.GetMetadata();

        /// Restore file if object has 'path' in metadata.
        auto path_entry = object_metadata.find("path");
        if (path_entry == object_metadata.end())
        {
            /// Such keys can remain after migration, we can skip them.
            LOG_WARNING(log, "Skip key {} because it doesn't have 'path' in metadata", key);
            continue;
        }

        const auto & path = path_entry->second;

        createDirectories(directoryPath(path));
        auto metadata = createMeta(path);
        auto relative_key = shrinkKey(source_path, key);

        /// Copy object if we restore to different bucket / path.
        if (bucket != source_bucket || remote_fs_root_path != source_path)
            copyObject(source_bucket, key, bucket, remote_fs_root_path + relative_key, head_result);

        metadata.addObject(relative_key, head_result.GetContentLength());
        metadata.save();

        LOG_TRACE(log, "Restored file {}", path);
    }
}

void DiskS3::restoreFileOperations(const RestoreInformation & restore_information)
{
    auto settings = current_settings.get();

    LOG_INFO(log, "Starting restore file operations for disk {}", name);

    /// Enable recording file operations if we restore to different bucket / path.
    bool send_metadata = bucket != restore_information.source_bucket || remote_fs_root_path != restore_information.source_path;

    std::set<String> renames;
    auto restore_file_operations = [this, &restore_information, &renames, &send_metadata](auto list_result)
    {
        const String rename = "rename";
        const String hardlink = "hardlink";

        for (const auto & row : list_result.GetContents())
        {
            const String & key = row.GetKey();

            const auto [revision, operation] = extractRevisionAndOperationFromKey(key);
            if (revision == UNKNOWN_REVISION)
            {
                LOG_WARNING(log, "Skip key {} with unknown revision", key);
                continue;
            }

            /// S3 ensures that keys will be listed in ascending UTF-8 bytes order (revision order).
            /// We can stop processing if revision of the object is already more than required.
            if (revision > restore_information.revision)
                return false;

            /// Keep original revision if restore to different bucket / path.
            if (send_metadata)
                revision_counter = revision - 1;

            auto object_metadata = headObject(restore_information.source_bucket, key).GetMetadata();
            if (operation == rename)
            {
                auto from_path = object_metadata["from_path"];
                auto to_path = object_metadata["to_path"];
                if (exists(from_path))
                {
                    moveFile(from_path, to_path, send_metadata);
                    LOG_TRACE(log, "Revision {}. Restored rename {} -> {}", revision, from_path, to_path);

                    if (restore_information.detached && isDirectory(to_path))
                    {
                        /// Sometimes directory paths are passed without trailing '/'. We should keep them in one consistent way.
                        if (!from_path.ends_with('/'))
                            from_path += '/';
                        if (!to_path.ends_with('/'))
                            to_path += '/';

                        /// Always keep latest actual directory path to avoid 'detaching' not existing paths.
                        auto it = renames.find(from_path);
                        if (it != renames.end())
                            renames.erase(it);

                        renames.insert(to_path);
                    }
                }
            }
            else if (operation == hardlink)
            {
                auto src_path = object_metadata["src_path"];
                auto dst_path = object_metadata["dst_path"];
                if (exists(src_path))
                {
                    createDirectories(directoryPath(dst_path));
                    createHardLink(src_path, dst_path, send_metadata);
                    LOG_TRACE(log, "Revision {}. Restored hardlink {} -> {}", revision, src_path, dst_path);
                }
            }
        }

        return true;
    };

    /// Execute.
    listObjects(restore_information.source_bucket, restore_information.source_path + "operations/", restore_file_operations);

    if (restore_information.detached)
    {
        Strings not_finished_prefixes{"tmp_", "delete_tmp_", "attaching_", "deleting_"};

        for (const auto & path : renames)
        {
            /// Skip already detached parts.
            if (path.find("/detached/") != std::string::npos)
                continue;

            /// Skip not finished parts. They shouldn't be in 'detached' directory, because CH wouldn't be able to finish processing them.
            fs::path directory_path(path);
            auto directory_name = directory_path.parent_path().filename().string();

            auto predicate = [&directory_name](String & prefix) { return directory_name.starts_with(prefix); };
            if (std::any_of(not_finished_prefixes.begin(), not_finished_prefixes.end(), predicate))
                continue;

            auto detached_path = pathToDetached(path);

            LOG_TRACE(log, "Move directory to 'detached' {} -> {}", path, detached_path);

            fs::path from_path = fs::path(metadata_path) / path;
            fs::path to_path = fs::path(metadata_path) / detached_path;
            if (path.ends_with('/'))
                to_path /= from_path.parent_path().filename();
            else
                to_path /= from_path.filename();
            fs::create_directories(to_path);
            fs::copy(from_path, to_path, fs::copy_options::recursive | fs::copy_options::overwrite_existing);
            fs::remove_all(from_path);
        }
    }

    LOG_INFO(log, "File operations restored for disk {}", name);
}

std::tuple<UInt64, String> DiskS3::extractRevisionAndOperationFromKey(const String & key)
{
    String revision_str;
    String operation;

    re2::RE2::FullMatch(key, key_regexp, &revision_str, &operation);

    return {(revision_str.empty() ? UNKNOWN_REVISION : static_cast<UInt64>(std::bitset<64>(revision_str).to_ullong())), operation};
}

String DiskS3::shrinkKey(const String & path, const String & key)
{
    if (!key.starts_with(path))
        throw Exception("The key " + key + " prefix mismatch with given " + path, ErrorCodes::LOGICAL_ERROR);

    return key.substr(path.length());
}

String DiskS3::revisionToString(UInt64 revision)
{
    return std::bitset<64>(revision).to_string();
}

String DiskS3::pathToDetached(const String & source_path)
{
    if (source_path.ends_with('/'))
        return fs::path(source_path).parent_path().parent_path() / "detached/";
    return fs::path(source_path).parent_path() / "detached/";
}

void DiskS3::onFreeze(const String & path)
{
    createDirectories(path);
    WriteBufferFromFile revision_file_buf(metadata_path + path + "revision.txt", 32);
    writeIntText(revision_counter.load(), revision_file_buf);
    revision_file_buf.finalize();
}

void DiskS3::applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context_, const String &, const DisksMap &)
{
    auto new_settings = settings_getter(config, "storage_configuration.disks." + name, context_);

    current_settings.set(std::move(new_settings));

    if (AsyncExecutor * exec = dynamic_cast<AsyncExecutor*>(&getExecutor()))
        exec->setMaxThreads(current_settings.get()->thread_pool_size);
}

DiskS3Settings::DiskS3Settings(
    const std::shared_ptr<Aws::S3::S3Client> & client_,
    size_t s3_max_single_read_retries_,
    size_t s3_min_upload_part_size_,
    size_t s3_max_single_part_upload_size_,
    size_t min_bytes_for_seek_,
    bool send_metadata_,
    int thread_pool_size_,
    int list_object_keys_size_,
    int objects_chunk_size_to_delete_)
    : client(client_)
    , s3_max_single_read_retries(s3_max_single_read_retries_)
    , s3_min_upload_part_size(s3_min_upload_part_size_)
    , s3_max_single_part_upload_size(s3_max_single_part_upload_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
    , send_metadata(send_metadata_)
    , thread_pool_size(thread_pool_size_)
    , list_object_keys_size(list_object_keys_size_)
    , objects_chunk_size_to_delete(objects_chunk_size_to_delete_)
{
}

}

#endif
