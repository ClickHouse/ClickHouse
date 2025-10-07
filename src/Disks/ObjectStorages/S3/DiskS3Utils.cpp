#include <Disks/ObjectStorages/S3/DiskS3Utils.h>

#if USE_AWS_S3
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>
#include <IO/S3/URI.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ObjectStorageKeysGeneratorPtr getKeyGenerator(
    const S3::URI & uri,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
{
    bool storage_metadata_write_full_object_key = DiskObjectStorageMetadata::getWriteFullObjectKeySetting();

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

}

#endif
