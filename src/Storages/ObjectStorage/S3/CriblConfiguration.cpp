#include <Storages/ObjectStorage/S3/CriblConfiguration.h>

#if USE_AWS_S3
#include <Disks/ObjectStorages/S3/CriblS3ObjectStorage.h>

#    include <Core/Settings.h>
#    include <Storages/NamedCollectionsHelpers.h>
#    include <Storages/StorageURL.h>
#    include <Storages/checkAndGetLiteralArgument.h>
#    include <Common/HTTPHeaderFilter.h>

#    include <Formats/FormatFactory.h>
#    include <IO/S3/getObjectInfo.h>

#    include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#    include <Disks/ObjectStorages/S3/diskSettings.h>

#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTIdentifier.h>
#    include <Parsers/ASTLiteral.h>

#    include <filesystem>
#    include <boost/algorithm/string.hpp>
#    include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool allow_archive_path_syntax;
extern const SettingsBool s3_create_new_file_on_insert;
extern const SettingsBool s3_ignore_file_doesnt_exist;
extern const SettingsUInt64 s3_list_object_keys_size;
extern const SettingsBool s3_skip_empty_files;
extern const SettingsBool s3_truncate_on_insert;
extern const SettingsBool s3_throw_on_zero_files_match;
extern const SettingsBool s3_validate_request_settings;
extern const SettingsSchemaInferenceMode schema_inference_mode;
extern const SettingsBool schema_inference_use_cache_for_s3;
}


CriblConfiguration::CriblConfiguration()
{

}

CriblConfiguration::~CriblConfiguration()
{

}


ObjectStoragePtr CriblConfiguration::createObjectStorage(ContextPtr context, bool /* is_readonly */) /// NOLINT
{
    assertInitialized();

    const auto & config = context->getConfigRef();
    const auto & settings = context->getSettingsRef();

    auto s3_settings = getSettings(config, "s3" /* config_prefix */, context, url.uri_str, settings[Setting::s3_validate_request_settings]);

    if (auto endpoint_settings = context->getStorageS3Settings().getSettings(url.uri.toString(), context->getUserName()))
    {
        s3_settings->auth_settings.updateIfChanged(endpoint_settings->auth_settings);
        s3_settings->request_settings.updateIfChanged(endpoint_settings->request_settings);
    }

    s3_settings->auth_settings.updateIfChanged(auth_settings);
    s3_settings->request_settings.updateIfChanged(request_settings);

    if (!headers_from_ast.empty())
    {
        s3_settings->auth_settings.headers.insert(
            s3_settings->auth_settings.headers.end(), headers_from_ast.begin(), headers_from_ast.end());
    }

    auto client = getClient(url, *s3_settings, context, /* for_disk_s3 */ false);
    auto key_generator = createObjectStorageKeysGeneratorAsIsWithPrefix(url.key);
    auto s3_capabilities = getCapabilitiesFromConfig(config, "s3");

    return std::make_shared<CriblS3ObjectStorage>(
        std::move(client), std::move(s3_settings), url, s3_capabilities, key_generator, "StorageS3", false);
}
}
#endif
