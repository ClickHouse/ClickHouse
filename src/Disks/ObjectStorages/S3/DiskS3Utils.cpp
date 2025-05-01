#include "DiskS3Utils.h"

#if USE_AWS_S3
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Core/ServerUUID.h>
#include <IO/S3/URI.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

ObjectStorageKeysGeneratorPtr getKeyGenerator(
    const S3::URI & uri,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
{
    bool storage_metadata_write_full_object_key = DiskObjectStorageMetadata::getWriteFullObjectKeySetting();
    bool send_metadata = config.getBool(config_prefix + ".send_metadata", false);

    if (send_metadata && storage_metadata_write_full_object_key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Wrong configuration in {}. "
                        "s3 does not supports feature 'send_metadata' with feature 'storage_metadata_write_full_object_key'.",
                        config_prefix);

    String object_key_compatibility_prefix = config.getString(config_prefix + ".key_compatibility_prefix", String());
    String object_key_template = config.getString(config_prefix + ".key_template", String());

    if (object_key_template.empty())
    {
        if (!object_key_compatibility_prefix.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Wrong configuration in {}. "
                            "Setting 'key_compatibility_prefix' can be defined only with setting 'key_template'.",
                            config_prefix);

        return createObjectStorageKeysGeneratorByPrefix(uri.key);
    }

    if (send_metadata)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Wrong configuration in {}. "
                        "s3 does not supports send_metadata with setting 'key_template'.",
                        config_prefix);

    if (!storage_metadata_write_full_object_key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Wrong configuration in {}. "
                        "Feature 'storage_metadata_write_full_object_key' has to be enabled in order to use setting 'key_template'.",
                        config_prefix);

    if (!uri.key.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Wrong configuration in {}. "
                        "URI.key is forbidden with settings 'key_template', use setting 'key_compatibility_prefix' instead'. "
                        "URI.key: '{}', bucket: '{}'. ",
                        config_prefix,
                        uri.key, uri.bucket);

    return createObjectStorageKeysGeneratorByTemplate(object_key_template);
}

static String getServerUUID()
{
    UUID server_uuid = ServerUUID::get();
    if (server_uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Server UUID is not initialized");
    return toString(server_uuid);
}

bool checkBatchRemove(S3ObjectStorage & storage)
{
    /// NOTE: Here we are going to write and later drop some key.
    /// We are using generateObjectKeyForPath() which returns random object key.
    /// That generated key is placed in a right directory where we should have write access.
    const String path = fmt::format("clickhouse_remove_objects_capability_{}", getServerUUID());
    const auto key = storage.generateObjectKeyForPath(path, {} /* key_prefix */);
    StoredObject object(key.serialize(), path);
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
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
        /// We don't have write access, therefore no information about batch remove.
        return true;
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
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
        return false;
    }
}
}

#endif
