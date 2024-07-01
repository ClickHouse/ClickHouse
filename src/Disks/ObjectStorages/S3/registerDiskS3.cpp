#include "config.h"

#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Disks/DiskFactory.h>

#if USE_AWS_S3

#include <base/getFQDNOrHostName.h>

#include <Disks/DiskLocal.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/ObjectStorages/MetadataStorageFromPlainObjectStorage.h>

#include <Core/ServerUUID.h>
#include <Common/Macros.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

class CheckAccess
{
public:
    static bool checkBatchRemove(S3ObjectStorage & storage, const String & key_with_trailing_slash)
    {
        /// NOTE: key_with_trailing_slash is the disk prefix, it is required
        /// because access is done via S3ObjectStorage not via IDisk interface
        /// (since we don't have disk yet).
        const String path = fmt::format("{}clickhouse_remove_objects_capability_{}", key_with_trailing_slash, getServerUUID());
        StoredObject object(path);
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
            return true; /// We don't have write access, therefore no information about batch remove.
        }
        try
        {
            /// Uses `DeleteObjects` request (batch delete).
            storage.removeObjects({object});
            return true;
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
            return false;
        }
    }

private:
    static String getServerUUID()
    {
        UUID server_uuid = ServerUUID::get();
        if (server_uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Server UUID is not initialized");
        return toString(server_uuid);
    }
};

}

void registerDiskS3(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /*map*/) -> DiskPtr
    {
        String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
        S3::URI uri(endpoint);
        if (!uri.key.ends_with('/'))
            uri.key.push_back('/');

        S3Capabilities s3_capabilities = getCapabilitiesFromConfig(config, config_prefix);
        std::shared_ptr<S3ObjectStorage> s3_storage;

        String type = config.getString(config_prefix + ".type");
        chassert(type == "s3" || type == "s3_plain");

        MetadataStoragePtr metadata_storage;
        auto settings = getSettings(config, config_prefix, context);
        auto client = getClient(config, config_prefix, context, *settings);
        if (type == "s3_plain")
        {
            /// send_metadata changes the filenames (includes revision), while
            /// s3_plain do not care about this, and expect that the file name
            /// will not be changed.
            ///
            /// And besides, send_metadata does not make sense for s3_plain.
            if (config.getBool(config_prefix + ".send_metadata", false))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "s3_plain does not supports send_metadata");

            s3_storage = std::make_shared<S3PlainObjectStorage>(std::move(client), std::move(settings), uri.version_id, s3_capabilities, uri.bucket, uri.endpoint);
            metadata_storage = std::make_shared<MetadataStorageFromPlainObjectStorage>(s3_storage, uri.key);
        }
        else
        {
            s3_storage = std::make_shared<S3ObjectStorage>(std::move(client), std::move(settings), uri.version_id, s3_capabilities, uri.bucket, uri.endpoint);
            auto [metadata_path, metadata_disk] = prepareForLocalMetadata(name, config, config_prefix, context);
            metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, uri.key);
        }

        /// NOTE: should we still perform this check for clickhouse-disks?
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        if (!skip_access_check)
        {
            /// If `support_batch_delete` is turned on (default), check and possibly switch it off.
            if (s3_capabilities.support_batch_delete && !CheckAccess::checkBatchRemove(*s3_storage, uri.key))
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

        DiskObjectStoragePtr s3disk = std::make_shared<DiskObjectStorage>(
            name,
            uri.key,
            type == "s3" ? "DiskS3" : "DiskS3Plain",
            std::move(metadata_storage),
            std::move(s3_storage),
            config,
            config_prefix);

        s3disk->startup(context, skip_access_check);

        return s3disk;
    };
    factory.registerDiskType("s3", creator);
    factory.registerDiskType("s3_plain", creator);
}

}

#else

void registerDiskS3(DB::DiskFactory &, bool /* global_skip_access_check */) {}

#endif
