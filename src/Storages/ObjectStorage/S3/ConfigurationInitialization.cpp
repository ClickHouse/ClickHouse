#include "config.h"

#if USE_AWS_S3

#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>

#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Interpreters/Context.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString http_client;
    extern const S3AuthSettingsString google_adc_client_id;
    extern const S3AuthSettingsString google_adc_client_secret;
    extern const S3AuthSettingsString google_adc_refresh_token;
    extern const S3AuthSettingsBool no_sign_request;
}

StorageParsedArguments S3StorageParsedArguments::extractBaseArguments()
{
    return std::move(static_cast<StorageParsedArguments &>(*this));
}

ConfigWithOptions fromS3NamedCollection(const NamedCollection & collection, ContextPtr context)
{
    S3StorageParsedArguments parsed_arguments;
    parsed_arguments.fromNamedCollection(collection, context);
    auto config = std::make_shared<StorageS3Configuration>(
        std::move(parsed_arguments.url),
        std::move(parsed_arguments.s3_settings),
        std::move(parsed_arguments.s3_capabilities),
        std::move(parsed_arguments.headers_from_ast));
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments(), config->getRawPath());
    return {config, std::move(table_options)};
}

ConfigWithOptions fromS3Disk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure)
{
    S3StorageParsedArguments parsed_arguments;
    auto disk = context->getDisk(disk_name);
    parsed_arguments.fromDisk(disk, args, context, with_structure);
    fs::path suffix = parsed_arguments.path_suffix;
    auto config = std::make_shared<StorageS3Configuration>(
        std::move(parsed_arguments.url),
        std::move(parsed_arguments.s3_settings),
        std::move(parsed_arguments.s3_capabilities),
        std::move(parsed_arguments.headers_from_ast));
    if (auto object_storage_disk = std::static_pointer_cast<DiskObjectStorage>(disk); object_storage_disk)
    {
        String path = object_storage_disk->getObjectStorage()->getCommonKeyPrefix();
        fs::path root = path;
        config->setRawPath(String(root / suffix));
    }
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments(), config->getRawPath());
    return {config, std::move(table_options)};
}

void setS3BigLakeCredentials(StorageS3Configuration & config, const String & client_id, const String & client_secret, const String & refresh_token)
{
    config.s3_settings->auth_settings[S3AuthSetting::http_client] = "gcp_oauth";
    config.s3_settings->auth_settings[S3AuthSetting::google_adc_client_id] = client_id;
    config.s3_settings->auth_settings[S3AuthSetting::google_adc_client_secret] = client_secret;
    config.s3_settings->auth_settings[S3AuthSetting::google_adc_refresh_token] = refresh_token;
}

ConfigWithOptions fromS3AST(ASTs & args, ContextPtr context, bool with_structure)
{
    S3StorageParsedArguments parsed_arguments;
    parsed_arguments.fromAST(args, context, with_structure);
    auto config = std::make_shared<StorageS3Configuration>(
        std::move(parsed_arguments.url),
        std::move(parsed_arguments.s3_settings),
        std::move(parsed_arguments.s3_capabilities),
        std::move(parsed_arguments.headers_from_ast));
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments(), config->getRawPath());
    return {config, std::move(table_options)};
}

}

#endif
