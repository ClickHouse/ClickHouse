#include "config.h"

#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Disks/DiskFactory.h>

#if USE_AWS_S3

#include <aws/core/client/DefaultRetryStrategy.h>
#include <base/getFQDNOrHostName.h>

#include <Disks/DiskRestartProxy.h>
#include <Disks/DiskLocal.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>
#include <IO/S3Common.h>

#include <Storages/StorageS3Settings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PATH_ACCESS_DENIED;
}

namespace
{

void checkWriteAccess(IDisk & disk)
{
    auto file = disk.writeFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    try
    {
        file->write("test", 4);
    }
    catch (...)
    {
        /// Log current exception, because finalize() can throw a different exception.
        tryLogCurrentException(__PRETTY_FUNCTION__);
        file->finalize();
        throw;
    }
}

void checkReadAccess(const String & disk_name, IDisk & disk)
{
    auto file = disk.readFile("test_acl");
    String buf(4, '0');
    file->readStrict(buf.data(), 4);
    if (buf != "test")
        throw Exception("No read access to S3 bucket in disk " + disk_name, ErrorCodes::PATH_ACCESS_DENIED);
}

void checkRemoveAccess(IDisk & disk)
{
    disk.removeFile("test_acl");
}

bool checkBatchRemoveIsMissing(S3ObjectStorage & storage, const String & key_with_trailing_slash)
{
    StoredObject object(key_with_trailing_slash + "_test_remove_objects_capability");
    try
    {
        auto file = storage.writeObject(object, WriteMode::Rewrite);
        file->write("test", 4);
        file->finalize();
    }
    catch (...)
    {
        try
        {
            storage.removeObject(object);
        }
        catch (...)
        {
        }
        return false; /// We don't have write access, therefore no information about batch remove.
    }
    try
    {
        /// Uses `DeleteObjects` request (batch delete).
        storage.removeObjects({object});
        return false;
    }
    catch (const Exception &)
    {
        try
        {
            storage.removeObject(object);
        }
        catch (...)
        {
        }
        return true;
    }
}

}

void registerDiskS3(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      const DisksMap & /*map*/) -> DiskPtr
    {
        S3::URI uri(Poco::URI(config.getString(config_prefix + ".endpoint")));

        if (uri.key.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No key in S3 uri: {}", uri.uri.toString());

        if (uri.key.back() != '/')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "S3 path must ends with '/', but '{}' doesn't.", uri.key);

        S3Capabilities s3_capabilities = getCapabilitiesFromConfig(config, config_prefix);
        std::shared_ptr<S3ObjectStorage> s3_storage;

        String type = config.getString(config_prefix + ".type");
        chassert(type == "s3" || type == "s3_plain");

        MetadataStoragePtr metadata_storage;
        if (type == "s3_plain")
        {
            s3_storage = std::make_shared<S3PlainObjectStorage>(
                getClient(config, config_prefix, context),
                getSettings(config, config_prefix, context),
                uri.version_id, s3_capabilities, uri.bucket, uri.endpoint);
            metadata_storage = std::make_shared<MetadataStorageFromPlainObjectStorage>(s3_storage, uri.key);
        }
        else
        {
            s3_storage = std::make_shared<S3ObjectStorage>(
                getClient(config, config_prefix, context),
                getSettings(config, config_prefix, context),
                uri.version_id, s3_capabilities, uri.bucket, uri.endpoint);

            auto [metadata_path, metadata_disk] = prepareForLocalMetadata(name, config, config_prefix, context);
            metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, uri.key);
        }

        bool skip_access_check = config.getBool(config_prefix + ".skip_access_check", false);

        if (!skip_access_check)
        {
            /// If `support_batch_delete` is turned on (default), check and possibly switch it off.
            if (s3_capabilities.support_batch_delete && checkBatchRemoveIsMissing(*s3_storage, uri.key))
            {
                LOG_WARNING(
                    &Poco::Logger::get("registerDiskS3"),
                    "Storage for disk {} does not support batch delete operations, "
                    "so `s3_capabilities.support_batch_delete` was automatically turned off during the access check. "
                    "To remove this message set `s3_capabilities.support_batch_delete` for the disk to `false`.",
                    name
                );
                s3_storage->setCapabilitiesSupportBatchDelete(false);
            }
        }

        bool send_metadata = config.getBool(config_prefix + ".send_metadata", false);
        uint64_t copy_thread_pool_size = config.getUInt(config_prefix + ".thread_pool_size", 16);

        std::shared_ptr<DiskObjectStorage> s3disk = std::make_shared<DiskObjectStorage>(
            name,
            uri.key,
            type == "s3" ? "DiskS3" : "DiskS3Plain",
            std::move(metadata_storage),
            std::move(s3_storage),
            send_metadata,
            copy_thread_pool_size);

        /// This code is used only to check access to the corresponding disk.
        if (!skip_access_check)
        {
            checkWriteAccess(*s3disk);
            checkReadAccess(name, *s3disk);
            checkRemoveAccess(*s3disk);
        }

        s3disk->startup(context);

        std::shared_ptr<IDisk> disk_result = s3disk;

        return std::make_shared<DiskRestartProxy>(disk_result);
    };
    factory.registerDiskType("s3", creator);
    factory.registerDiskType("s3_plain", creator);
}

}

#else

void registerDiskS3(DiskFactory &) {}

#endif
