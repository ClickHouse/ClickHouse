#include <Disks/ObjectStorages/S3/DiskS3Utils.h>

#if USE_AWS_S3
#include <Common/Macros.h>
#include <Disks/ObjectStorages/DiskObjectStorageMetadata.h>
#include <Interpreters/Context.h>
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
    String object_key_compatibility_prefix = config.getString(config_prefix + ".key_compatibility_prefix", String());
    String object_key_template = config.getString(config_prefix + ".key_template", String());

    Macros::MacroExpansionInfo info;
    info.ignore_unknown = true;
    info.expand_special_macros_only = true;
    info.replica = Context::getGlobalContextInstance()->getMacros()->tryGetValue("replica");
    object_key_compatibility_prefix = Context::getGlobalContextInstance()->getMacros()->expand(object_key_compatibility_prefix, info);
    info.level = 0;
    object_key_template = Context::getGlobalContextInstance()->getMacros()->expand(object_key_template, info);

    if (object_key_template.empty())
    {
        if (!object_key_compatibility_prefix.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Wrong configuration in {}. "
                            "Setting 'key_compatibility_prefix' can be defined only with setting 'key_template'.",
                            config_prefix);

        return createObjectStorageKeysGeneratorByPrefix(uri.key);
    }

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
