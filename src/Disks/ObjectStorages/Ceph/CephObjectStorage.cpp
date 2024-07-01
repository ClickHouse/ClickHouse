#include "CephObjectStorage.h"
#include <ctime>
#include <fcntl.h>
#include <rados/librados.hpp>

#include "Common/ObjectStorageKey.h"
#include <IO/Ceph/RadosIO.h>

#if USE_CEPH

#include <IO/S3Common.h>
#include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/WriteBufferFromCeph.h>
#include <IO/ReadBufferFromCeph.h>
#include <Interpreters/Context.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/MultiVersion.h>
#include <Common/Macros.h>


namespace ProfileEvents
{
    extern const Event S3DeleteObjects;
    extern const Event S3ListObjects;
    extern const Event DiskS3DeleteObjects;
    extern const Event DiskS3ListObjects;
}

namespace CurrentMetrics
{
    extern const Metric ObjectStorageS3Threads;
    extern const Metric ObjectStorageS3ThreadsActive;
    extern const Metric ObjectStorageS3ThreadsScheduled;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int CEPH_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

// class S3IteratorAsync final : public IObjectStorageIteratorAsync
// {
// public:
//     S3IteratorAsync(
//         const std::string & bucket_,
//         const std::string & path_prefix,
//         std::shared_ptr<const S3::Client> client_,
//         size_t max_list_size)
//         : IObjectStorageIteratorAsync(
//             CurrentMetrics::ObjectStorageS3Threads,
//             CurrentMetrics::ObjectStorageS3ThreadsActive,
//             CurrentMetrics::ObjectStorageS3ThreadsScheduled,
//             "ListObjectS3")
//         , client(client_)
//         , request(std::make_unique<S3::ListObjectsV2Request>())
//     {
//         request->SetBucket(bucket_);
//         request->SetPrefix(path_prefix);
//         request->SetMaxKeys(static_cast<int>(max_list_size));
//     }

//     ~S3IteratorAsync() override
//     {
//         /// Deactivate background threads before resetting the request to avoid data race.
//         deactivate();
//         request.reset();
//         client.reset();
//     }

// private:
//     bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
//     {
//         ProfileEvents::increment(ProfileEvents::S3ListObjects);
//         ProfileEvents::increment(ProfileEvents::DiskS3ListObjects);

//         auto outcome = client->ListObjectsV2(*request);

//         /// Outcome failure will be handled on the caller side.
//         if (outcome.IsSuccess())
//         {
//             request->SetContinuationToken(outcome.GetResult().GetNextContinuationToken());

//             auto objects = outcome.GetResult().GetContents();
//             for (const auto & object : objects)
//             {
//                 ObjectMetadata metadata{static_cast<uint64_t>(object.GetSize()), Poco::Timestamp::fromEpochTime(object.GetLastModified().Seconds()), {}};
//                 batch.emplace_back(std::make_shared<RelativePathWithMetadata>(object.GetKey(), std::move(metadata)));
//             }

//             /// It returns false when all objects were returned
//             return outcome.GetResult().GetIsTruncated();
//         }

//         throw S3Exception(outcome.GetError().GetErrorType(),
//                           "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
//                           quoteString(request->GetBucket()), quoteString(request->GetPrefix()),
//                           backQuote(outcome.GetError().GetExceptionName()), quoteString(outcome.GetError().GetMessage()));
//     }

//     std::shared_ptr<const S3::Client> client;
//     std::unique_ptr<S3::ListObjectsV2Request> request;
// };

}

bool CephObjectStorage::exists(const StoredObject & object) const
{
    auto settings_ptr = ceph_settings.get();
    return base_io->exists(object.remote_path);
}

std::unique_ptr<ReadBufferFromFileBase> CephObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    ReadSettings disk_read_settings = patchSettings(read_settings);
    auto global_context = Context::getGlobalContextInstance();

    auto settings_ptr = ceph_settings.get();

    auto read_buffer_creator =
        [this, settings_ptr, disk_read_settings]
        (bool restricted_seek, const StoredObject & object_) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        Ceph::RadosIO io_impl(rados, endpoint.pool);
        return std::make_unique<ReadBufferFromCeph>(
            std::make_unique<Ceph::RadosIO>(rados, endpoint.pool),
            object_.remote_path,
            disk_read_settings,
            /* use_external_buffer */true,
            /* offset */0,
            /* read_until_position */0,
            restricted_seek);
    };

    switch (read_settings.remote_fs_method)
    {
        case RemoteFSReadMethod::read:
        {
            return std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator),
                objects,
                "ceph:" + endpoint.pool + "/",
                disk_read_settings,
                global_context->getFilesystemCacheLog(),
                /* use_external_buffer */false);
        }
        case RemoteFSReadMethod::threadpool:
        {
            auto impl = std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator),
                objects,
                "ceph:" + endpoint.pool + "/",
                disk_read_settings,
                global_context->getFilesystemCacheLog(),
                /* use_external_buffer */true);

            auto & reader = global_context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
            return std::make_unique<AsynchronousBoundedReadBuffer>(
                std::move(impl), reader, disk_read_settings,
                global_context->getAsyncReadCounters(),
                global_context->getFilesystemReadPrefetchesLog());
        }
    }
}

std::unique_ptr<ReadBufferFromFileBase> CephObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto settings_ptr = ceph_settings.get();
    return std::make_unique<ReadBufferFromCeph>(
        std::make_unique<Ceph::RadosIO>(rados, endpoint.pool),
        object.remote_path,
        read_settings);
}

std::unique_ptr<WriteBufferFromFileBase> CephObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> /*attributes*/,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    WriteSettings disk_write_settings = IObjectStorage::patchSettings(write_settings);

    return std::make_unique<WriteBufferFromCeph>(
        std::make_unique<Ceph::RadosIO>(rados, endpoint.pool),
        object.remote_path,
        write_settings,
        buf_size,
        mode);
}


ObjectStorageIteratorPtr CephObjectStorage::iterate(const std::string & path_prefix, size_t max_keys) const
{
    auto settings_ptr = ceph_settings.get();
    return std::make_shared<S3IteratorAsync>(uri.bucket, path_prefix, client.get(), max_keys);
}

void CephObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    auto settings_ptr = ceph_settings.get();
    if (max_keys)
        request.SetMaxKeys(static_cast<int>(max_keys));
    else
        request.SetMaxKeys(settings_ptr->list_object_keys_size);

    do
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        ProfileEvents::increment(ProfileEvents::DiskS3ListObjects);

        outcome = client.get()->ListObjectsV2(request);
        throwIfError(outcome);

        auto result = outcome.GetResult();
        auto objects = result.GetContents();

        if (objects.empty())
            break;

        for (const auto & object : objects)
            children.emplace_back(std::make_shared<RelativePathWithMetadata>(
                object.GetKey(),
                ObjectMetadata{
                    static_cast<uint64_t>(object.GetSize()),
                    Poco::Timestamp::fromEpochTime(object.GetLastModified().Seconds()),
                    {}}));

        if (max_keys)
        {
            size_t keys_left = max_keys - children.size();
            if (keys_left <= 0)
                break;
            request.SetMaxKeys(static_cast<int>(keys_left));
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
    } while (outcome.GetResult().GetIsTruncated());
}

void CephObjectStorage::removeObjectImpl(const StoredObject & object, bool if_exists)
{
    base_io->remove(object.remote_path, if_exists);
}

void CephObjectStorage::removeObjectsImpl(const StoredObjects & objects, bool if_exists)
{
    for (const auto & object : objects)
        removeObjectImpl(object, if_exists);
}

void CephObjectStorage::removeObject(const StoredObject & object)
{
    removeObjectImpl(object, false);
}

void CephObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    removeObjectImpl(object, true);
}

void CephObjectStorage::removeObjects(const StoredObjects & objects)
{
    removeObjectsImpl(objects, false);
}

void CephObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    removeObjectsImpl(objects, true);
}

std::optional<ObjectMetadata> CephObjectStorage::tryGetObjectMetadata(const std::string & path) const
{
    auto settings_ptr = ceph_settings.get();
    auto object_info = S3::getObjectInfo(
        *client.get(), uri.bucket, path, {}, /* with_metadata= */ true, /* throw_on_error= */ false);

    if (object_info.size == 0 && object_info.last_modification_time == 0 && object_info.metadata.empty())
        return {};

    ObjectMetadata result;
    result.size_bytes = object_info.size;
    result.last_modified = Poco::Timestamp::fromEpochTime(object_info.last_modification_time);
    result.attributes = object_info.metadata;

    return result;
}

ObjectMetadata CephObjectStorage::getObjectMetadata(const std::string & path) const
{
    auto settings_ptr = ceph_settings.get();
    size_t sz;
    struct timespec mtime;
    std::map<String, String> attrs;
    base_io->getMetadata(path, &sz, &mtime, attrs);

    ObjectMetadata result;
    result.size_bytes = sz;
    result.last_modified = Poco::Timestamp::fromEpochTime(mtime.tv_sec);
    result.attributes = std::move(metadata);

    return result;
}

void CephObjectStorage::copyObjectToAnotherObjectStorage( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    IObjectStorage::copyObjectToAnotherObjectStorage(object_from, object_to, read_settings, write_settings, object_storage_to, object_to_attributes);
}

void CephObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> object_to_attributes)
{
    ReadBufferFromCeph from(rados, endpoint.pool, object_from.remote_path, read_settings);
    WriteBufferFromCeph to(rados, endpoint.pool, object_to.remote_path, write_settings);
    copyData(from, to);
}

void CephObjectStorage::setNewSettings(std::unique_ptr<CephObjectStorageSettings> && ceph_settings_)
{
    ceph_settings.set(std::move(ceph_settings_));
}

void CephObjectStorage::shutdown()
{
    /// This call stops any next retry attempts for ongoing S3 requests.
    /// If S3 request is failed and the method below is executed S3 client immediately returns the last failed S3 request outcome.
    /// If S3 is healthy nothing wrong will be happened and S3 requests will be processed in a regular way without errors.
    /// This should significantly speed up shutdown process if S3 is unhealthy.
    const_cast<S3::Client &>(*client.get()).DisableRequestProcessing();
}

void CephObjectStorage::startup()
{
    /// Need to be enabled if it was disabled during shutdown() call.
    const_cast<S3::Client &>(*client.get()).EnableRequestProcessing();
}

void CephObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr context,
    const ApplyNewSettingsOptions & options)
{
    auto settings_from_config = getSettings(config, config_prefix, context, uri.uri_str, context->getSettingsRef().s3_validate_request_settings);
    auto modified_settings = std::make_unique<CephObjectStorageSettings>(*ceph_settings.get());
    modified_settings->auth_settings.updateIfChanged(settings_from_config->auth_settings);
    modified_settings->request_settings.updateIfChanged(settings_from_config->request_settings);

    if (auto endpoint_settings = context->getStorageS3Settings().getSettings(uri.uri.toString(), context->getUserName()))
    {
        modified_settings->auth_settings.updateIfChanged(endpoint_settings->auth_settings);
        modified_settings->request_settings.updateIfChanged(endpoint_settings->request_settings);
    }

    auto current_settings = ceph_settings.get();
    if (options.allow_client_change
        && (current_settings->auth_settings.hasUpdates(modified_settings->auth_settings) || for_disk_s3))
    {
        auto new_client = getClient(uri, *modified_settings, context, for_disk_s3);
        client.set(std::move(new_client));
    }
    ceph_settings.set(std::move(modified_settings));
}

std::unique_ptr<IObjectStorage> CephObjectStorage::cloneObjectStorage(
    const std::string & new_namespace,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    auto new_ceph_settings = getSettings(config, config_prefix, context, uri.uri_str, settings.s3_validate_request_settings);
    auto new_client = getClient(uri, *new_ceph_settings, context, for_disk_s3);

    auto new_uri{uri};
    new_uri.bucket = new_namespace;

    return std::make_unique<CephObjectStorage>(
        std::move(new_client), std::move(new_ceph_settings), new_uri, s3_capabilities, key_generator, disk_name);
}

ObjectStorageKey CephObjectStorage::generateObjectKeyForPath(const std::string & path) const
{
    if (!key_generator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key generator is not set");

    return key_generator->generate(path, /* is_directory */ false);
}

}

#endif
