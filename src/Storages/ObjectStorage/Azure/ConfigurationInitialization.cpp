#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>

#include <Interpreters/Context.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <azure/identity/client_secret_credential.hpp>

namespace DB
{

StorageParsedArguments AzureStorageParsedArguments::extractBaseArguments()
{
    return std::move(static_cast<StorageParsedArguments &>(*this));
}

StorageAzureConfiguration::StorageAzureConfiguration(
    Path blob_path_,
    AzureBlobStorage::ConnectionParams connection_params_,
    DiskPtr disk_)
{
    blob_path = std::move(blob_path_);
    connection_params = std::move(connection_params_);
    disk = std::move(disk_);
    setPaths({blob_path});
}

ConfigWithOptions fromAzureNamedCollection(const NamedCollection & collection, ContextPtr context)
{
    AzureStorageParsedArguments parsed_arguments;
    parsed_arguments.fromNamedCollection(collection, context);
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments());
    auto config = std::make_shared<StorageAzureConfiguration>(
        parsed_arguments.blob_path,
        parsed_arguments.connection_params);
    return {config, std::move(table_options)};
}

ConfigWithOptions fromAzureAST(ASTs & engine_args, ContextPtr context, bool with_structure)
{
    AzureStorageParsedArguments parsed_arguments;
    parsed_arguments.fromAST(engine_args, context, with_structure);
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments());
    auto config = std::make_shared<StorageAzureConfiguration>(
        parsed_arguments.blob_path,
        parsed_arguments.connection_params);
    return {config, std::move(table_options)};
}

ConfigWithOptions fromAzureOneLake(
    ASTs & args, ContextPtr context, const String & client_id, const String & client_secret, const String & tenant_id)
{
    AzureStorageParsedArguments parsed_arguments;
    parsed_arguments.initializeForOneLake(args, context);
    parsed_arguments.connection_params.auth_method = std::make_shared<Azure::Identity::ClientSecretCredential>(
        tenant_id, client_id, client_secret);
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments());
    auto config = std::make_shared<StorageAzureConfiguration>(
        parsed_arguments.blob_path,
        parsed_arguments.connection_params);
    StorageObjectStorageConfiguration::postInitializeExisting(*config, table_options, context);
    return {config, std::move(table_options)};
}

ConfigWithOptions fromAzureDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure)
{
    AzureStorageParsedArguments parsed_arguments;
    auto disk = context->getDisk(disk_name);
    parsed_arguments.fromDisk(disk, args, context, with_structure);
    auto table_options = tableOptionsFromParsedArguments(parsed_arguments.extractBaseArguments());
    auto config = std::make_shared<StorageAzureConfiguration>(
        parsed_arguments.blob_path,
        parsed_arguments.connection_params,
        disk);
    table_options.setPathForRead(parsed_arguments.blob_path.path + "/");
    config->setPaths({parsed_arguments.blob_path.path + "/"});
    return {config, std::move(table_options)};
}

}

#endif
