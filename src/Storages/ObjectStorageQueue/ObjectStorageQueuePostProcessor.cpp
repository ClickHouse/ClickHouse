#include <Common/ProfileEvents.h>
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/S3/BlobStorageLogWriter.h>
#include <IO/S3/copyS3File.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/WriteSettings.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueuePostProcessor.h>


namespace ProfileEvents
{
    extern const Event ObjectStorageQueueMovedObjects;
    extern const Event ObjectStorageQueueRemovedObjects;
    extern const Event ObjectStorageQueueTaggedObjects;
}

namespace DB
{

namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString secret_access_key;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ObjectStorageQueuePostProcessor::ObjectStorageQueuePostProcessor(
    ContextPtr context_,
    ObjectStorageType type_,
    ObjectStoragePtr object_storage_,
    String engine_name_,
    const ObjectStorageQueueTableMetadata & table_metadata_)
    : WithContext(context_)
    , type(type_)
    , object_storage(object_storage_)
    , engine_name(engine_name_)
    , table_metadata(table_metadata_)
    , log(getLogger("ObjectStorageQueuePostProcessor"))
{ }

void ObjectStorageQueuePostProcessor::process(const StoredObjects & objects) const
{
    const ObjectStorageQueueAction after_processing_action = table_metadata.after_processing.load();
    if (after_processing_action == ObjectStorageQueueAction::DELETE)
    {
        /// We do need to apply after-processing action before committing requests to keeper.
        /// See explanation in ObjectStorageQueueSource::FileIterator::nextImpl().
        object_storage->removeObjectsIfExist(objects);
        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueRemovedObjects, objects.size());
    }
    else if (after_processing_action == ObjectStorageQueueAction::MOVE)
    {
        switch (type)
        {
            case ObjectStorageType::Azure:
                moveAzureBlobs(objects);
                break;
            case ObjectStorageType::S3:
                moveS3Objects(objects);
                break;
            default:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "after processing move not allowed for storage type {}, only Azure and S3 supported",
                    type);
        }
    }
    else if (after_processing_action == ObjectStorageQueueAction::TAG)
    {
        const String & tag_key = table_metadata.after_processing_tag_key;
        const String & tag_value = table_metadata.after_processing_tag_value;
        LOG_INFO(log, "executing TAG action in ObjectStorage Queue commit stage, {} = {}", tag_key, tag_value);
        object_storage->tagObjects(objects, tag_key, tag_value);
        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTaggedObjects, objects.size());
    }
    else if (after_processing_action != ObjectStorageQueueAction::KEEP)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "unsupported after_processing action {}",
            ObjectStorageQueueTableMetadata::actionToString(after_processing_action));
    }

}

static StoredObject applyMovePrefixIfPresent(const StoredObject & src, const String & move_prefix)
{
    if (move_prefix.empty())
    {
        return src;
    }
    const String file_name = fileName(src.remote_path);
    const String remote_path = fs::path(move_prefix) / file_name;
    return StoredObject(remote_path);
}

#if USE_AZURE_BLOB_STORAGE

static AzureBlobStorage::ConnectionParams getAzureConnectionParams(
    const String & connection_url,
    const String & container_name,
    const ContextPtr & local_context)
{
    AzureBlobStorage::ConnectionParams connection_params;
    auto request_settings = AzureBlobStorage::getRequestSettings(local_context->getSettingsRef());

    AzureBlobStorage::processURL(connection_url, container_name, connection_params.endpoint, connection_params.auth_method);
    connection_params.client_options = AzureBlobStorage::getClientOptions(local_context, local_context->getSettingsRef(), *request_settings, /*for_disk=*/ false);

    return connection_params;
}

#endif

void ObjectStorageQueuePostProcessor::moveWithinBucket(const StoredObjects & objects, const String & move_prefix) const
{
    auto read_settings = getReadSettings();
    auto write_settings = getWriteSettings();

    for (const auto & object_from : objects)
    {
        auto object_to = applyMovePrefixIfPresent(object_from, move_prefix);
        LOG_TRACE(log, "copying object {} to {}", object_from.remote_path, object_to.remote_path);
        object_storage->copyObject(
            object_from,
            object_to,
            read_settings,
            write_settings);
        LOG_INFO(log, "removing object {}", object_from.remote_path);
        object_storage->removeObjectIfExists(object_from);
    }
    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMovedObjects, objects.size());
}

void ObjectStorageQueuePostProcessor::moveS3Objects(const StoredObjects & objects) const
{
    const String & move_uri = table_metadata.after_processing_move_uri;
    const String & move_access_key_id = table_metadata.after_processing_move_access_key_id;
    const String & move_secret_access_key = table_metadata.after_processing_move_secret_access_key;
    const String & move_prefix = table_metadata.after_processing_move_prefix;

    if (!move_uri.empty() || !move_access_key_id.empty() || !move_secret_access_key.empty())
    {
        if (move_uri.empty() || move_access_key_id.empty() || move_secret_access_key.empty())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "not enough settings to move S3 objects");
        }

        if (auto * s3_storage = dynamic_cast<S3ObjectStorage * >(object_storage.get()); s3_storage != nullptr)
        {
            auto src_client = s3_storage->getS3StorageClient();
            auto settings = std::make_unique<S3Settings>();
            auto contextPtr = getContext();
            settings->loadFromConfig(
                contextPtr->getConfigRef(),
                /* config_prefix */ "s3",
                contextPtr->getSettingsRef()
            );
            settings->auth_settings[S3AuthSetting::access_key_id] = move_access_key_id;
            settings->auth_settings[S3AuthSetting::secret_access_key] = move_secret_access_key;
            std::shared_ptr<S3::Client> dst_client = getClient(
                move_uri,
                *settings.get(),
                contextPtr,
                /* for_disk_s3 */ true
            );
            auto dst_uri = S3::URI(move_uri);
            auto read_settings = getReadSettings();
            const auto read_settings_to_use = s3_storage->patchSettings(read_settings);
            auto scheduler = threadPoolCallbackRunnerUnsafe<void>(
                IObjectStorage::getThreadPoolWriter(),
                "ObjStorQueue_move_s3");
            for (const auto & object_from : objects)
            {
                const String src_bucket = s3_storage->getObjectsNamespace();
                size_t object_size = S3::getObjectSize(
                    *src_client,
                    src_bucket,
                    object_from.remote_path);
                auto object_to = applyMovePrefixIfPresent(object_from, move_prefix);

                LOG_INFO(log, "copying {} (size {}B) to bucket {}", object_from.remote_path, object_size, dst_uri.bucket);
                copyS3File(
                    src_client,
                    /*src_bucket=*/ src_bucket,
                    /*src_key=*/ object_from.remote_path,
                    /*src_offset=*/ 0,
                    /*src_size=*/ object_size,
                    /*dest_s3_client=*/ dst_client,
                    /*dest_bucket=*/ dst_uri.bucket,
                    /*dest_key=*/ object_to.remote_path,
                    /*settings=*/ settings->request_settings,
                    /*read_settings=*/ read_settings_to_use,
                    BlobStorageLogWriter::create(getName()),
                    scheduler,
                    /*fallback_file_reader=*/ [&]{
                        return s3_storage->readObject(object_from, read_settings_to_use);
                    }
                    /*object_metadata=*/);

                LOG_INFO(log, "removing object {}", object_from.remote_path);
                object_storage->removeObjectIfExists(object_from);
            }
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMovedObjects, objects.size());
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "underlying storage is not S3");
        }
    }
    else if (!move_prefix.empty())
    {
        moveWithinBucket(objects, move_prefix);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "no settings to move S3 objects");
    }
}

void ObjectStorageQueuePostProcessor::moveAzureBlobs(const StoredObjects & objects) const
{
#if USE_AZURE_BLOB_STORAGE
    const String & move_connection_string = table_metadata.after_processing_move_connection_string;
    const String & move_container = table_metadata.after_processing_move_container;
    const String & move_prefix = table_metadata.after_processing_move_prefix;

    if (!move_connection_string.empty() || !move_container.empty())
    {
        if (move_connection_string.empty() || move_container.empty())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "not enough settings to move Azure blobs");
        }

        if (auto * azure_storage = dynamic_cast<AzureObjectStorage * >(object_storage.get()); azure_storage != nullptr)
        {
            auto contextPtr = getContext();
            std::shared_ptr<const AzureBlobStorage::ContainerClient> src_client = azure_storage->getAzureBlobStorageClient();
            auto connection_params = getAzureConnectionParams(
                move_connection_string,
                move_container,
                contextPtr);
            const bool is_readonly = true;
            std::shared_ptr<AzureBlobStorage::ContainerClient> dst_client = AzureBlobStorage::getContainerClient(
                connection_params,
                is_readonly);

            for (const auto & object_from : objects)
            {
                Azure::Storage::Blobs::BlobClient blobClient = src_client->GetBlobClient(object_from.remote_path);
                auto properties = blobClient.GetProperties().Value;
                auto blob_size = properties.BlobSize;
                auto object_to = applyMovePrefixIfPresent(object_from, move_prefix);
                auto settings = azure_storage->getSettings();
                auto read_settings = getReadSettings();
                const auto read_settings_to_use = azure_storage->patchSettings(read_settings);
                bool same_credentials = compareAzureAuthMethod(azure_storage->getAzureBlobStorageAuthMethod(), connection_params.auth_method);
                auto scheduler = threadPoolCallbackRunnerUnsafe<void>(
                    IObjectStorage::getThreadPoolWriter(),
                    "ObjStorQueue_move_azure");

                LOG_INFO(log, "copying {} (size {}B) to container {}", object_from.remote_path, blob_size, move_container);
                copyAzureBlobStorageFile(
                    src_client,
                    dst_client,
                    connection_params.getContainer(),
                    /* src_blob */ object_from.remote_path,
                    /* src_offset */ 0,
                    blob_size,
                    move_container,
                    /* dest_blob */ object_to.remote_path,
                    settings,
                    read_settings,
                    same_credentials,
                    scheduler
                );
                LOG_INFO(log, "removing object {}", object_from.remote_path);
                object_storage->removeObjectIfExists(object_from);
            }
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueMovedObjects, objects.size());
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "underlying storage is not Azure");
        }
    }
    else if (!move_prefix.empty())
    {
        moveWithinBucket(objects, move_prefix);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "no settings to move Azure blobs");
    }
#else
    (void) objects;
#endif
}

}
