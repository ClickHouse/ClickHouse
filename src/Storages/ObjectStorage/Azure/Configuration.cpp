#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Poco/URI.h>

#if USE_AZURE_BLOB_STORAGE

#include <Common/assert_cast.h>
#include <azure/storage/common/storage_credential.hpp>
#include <Storages/NamedCollectionsHelpers.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Formats/FormatFactory.h>
#include <azure/storage/blobs.hpp>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/identity/workload_identity_credential.hpp>
#include <azure/identity/client_secret_credential.hpp>
#include <Core/Settings.h>
#include <Common/RemoteHostFilter.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ObjectStorage/Utils.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool azure_create_new_file_on_insert;
    extern const SettingsBool azure_ignore_file_doesnt_exist;
    extern const SettingsUInt64 azure_list_object_keys_size;
    extern const SettingsBool azure_skip_empty_files;
    extern const SettingsBool azure_throw_on_zero_files_match;
    extern const SettingsBool azure_truncate_on_insert;
    extern const SettingsSchemaInferenceMode schema_inference_mode;
    extern const SettingsBool schema_inference_use_cache_for_azure;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

const std::unordered_set<std::string_view> required_configuration_keys = {
    "blob_path",
    "container",
};

const std::unordered_set<std::string_view> optional_configuration_keys = {
    "format",
    "compression",
    "structure",
    "compression_method",
    "account_name",
    "account_key",
    "connection_string",
    "storage_account_url",
    "partition_strategy",
    "partition_columns_in_data_file",
    "client_id",
    "tenant_id",
};

void StorageAzureConfiguration::check(ContextPtr context)
{
    auto url = Poco::URI(connection_params.getConnectionURL());
    context->getGlobalContext()->getRemoteHostFilter().checkURL(url);
    StorageObjectStorageConfiguration::check(context);
}

StorageObjectStorageQuerySettings StorageAzureConfiguration::getQuerySettings(const ContextPtr & context) const
{
    const auto & settings = context->getSettingsRef();
    return StorageObjectStorageQuerySettings{
        .truncate_on_insert = settings[Setting::azure_truncate_on_insert],
        .create_new_file_on_insert = settings[Setting::azure_create_new_file_on_insert],
        .schema_inference_use_cache = settings[Setting::schema_inference_use_cache_for_azure],
        .schema_inference_mode = settings[Setting::schema_inference_mode],
        .skip_empty_files = settings[Setting::azure_skip_empty_files],
        .list_object_keys_size = settings[Setting::azure_list_object_keys_size],
        .throw_on_zero_files_match = settings[Setting::azure_throw_on_zero_files_match],
        .ignore_non_existent_file = settings[Setting::azure_ignore_file_doesnt_exist],
    };
}

ObjectStoragePtr StorageAzureConfiguration::createObjectStorage(ContextPtr context, bool is_readonly) /// NOLINT
{
    assertInitialized();

    auto settings = AzureBlobStorage::getRequestSettings(context->getSettingsRef());
    auto client = AzureBlobStorage::getContainerClient(connection_params, is_readonly);

    return std::make_unique<AzureObjectStorage>(
        "AzureBlobStorage",
        connection_params.auth_method,
        std::move(client),
        std::move(settings),
        connection_params,
        connection_params.getContainer(),
        connection_params.getConnectionURL(),
        /*common_key_prefix*/ "");
}

static AzureBlobStorage::ConnectionParams getConnectionParams(
    const String & connection_url,
    const String & container_name,
    const std::optional<String> & account_name,
    const std::optional<String> & account_key,
    const std::optional<String> & client_id,
    const std::optional<String> & tenant_id,
    const ContextPtr & local_context)
{
    AzureBlobStorage::ConnectionParams connection_params;
    auto request_settings = AzureBlobStorage::getRequestSettings(local_context->getSettingsRef());

    if (client_id || tenant_id)
    {
        if (!client_id || !tenant_id)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Both 'client_id' and 'tenant_id' need to be provided, but '{}' is missing", client_id ? "tenant_id" : "client_id");

        connection_params.endpoint.storage_account_url = connection_url;
        connection_params.endpoint.container_name = container_name;
        Azure::Identity::WorkloadIdentityCredentialOptions options;
        options.ClientId = *client_id;
        options.TenantId = *tenant_id;
        connection_params.auth_method = std::make_shared<Azure::Identity::WorkloadIdentityCredential>(options);
    }

    if (account_name || account_key)
    {
        if (connection_params.auth_method.index() != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Both 'extra_credentials' with 'client_id' and 'tenant_id' and account credentials provided. Choose only one");

        if (!account_name || !account_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Both 'account_name' and 'account_key' need to be provided, but '{}' is missing", account_name ? "account_key" : "account_name");

        connection_params.endpoint.storage_account_url = connection_url;
        connection_params.endpoint.container_name = container_name;
        connection_params.auth_method = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(*account_name, *account_key);
    }

    if (connection_params.auth_method.index() == 0)
    {
        AzureBlobStorage::processURL(connection_url, container_name, connection_params.endpoint, connection_params.auth_method);
    }

    connection_params.client_options = AzureBlobStorage::getClientOptions(local_context, local_context->getSettingsRef(), *request_settings, /*for_disk=*/ false);
    return connection_params;
}

void StorageAzureConfiguration::fillBlobsFromURLCommon(String & connection_url, const String & suffix, const String & full_suffix)
{
    String container_name;

    auto pos_container = connection_url.find(suffix);

    if (pos_container != std::string::npos)
    {
        String container_blob_path = connection_url.substr(pos_container+5);
        connection_url = connection_url.substr(0,pos_container+4);
        container_name = connection_url.substr(pos_container+4);
        auto pos_blob_path = container_blob_path.find('/');

        if (pos_blob_path != std::string::npos)
        {
            container_name = container_blob_path.substr(0, pos_blob_path);
            blob_path = container_blob_path.substr(pos_blob_path);
        }
    }

    /// Added for Unity Catalog on top of AzureBlobStorage
    // Sample abfss url : abfss://mycontainer@mydatalakestorage.dfs.core.windows.net/subdirectory/file.txt
    if (connection_url.starts_with("abfss"))
    {
        auto pos_slash = connection_url.find("://");
        auto pos_at = connection_url.find('@');
        auto pos_dot = connection_url.find('.');
        auto pos_net = connection_url.find(suffix);

        if (pos_slash == std::string::npos || pos_at == std::string::npos|| pos_dot == std::string::npos || pos_net == std::string::npos
            || pos_at-pos_slash-3 <= 0 || pos_dot-pos_at-1 <= 0)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect url format for a abfss url {}", connection_url);
        }
        auto container_name_abfss = connection_url.substr(pos_slash+3, pos_at-pos_slash-3);
        auto name = connection_url.substr(pos_at+1, pos_dot-pos_at-1);

        connection_params.endpoint.storage_account_url = "https://" + name + full_suffix;

        if (!container_name.empty())
        {
            blob_path.path = container_name + blob_path.path;
        }
        connection_params.endpoint.container_name = container_name_abfss;
    }
    blobs_paths = {blob_path};
}


void StorageAzureConfiguration::fromNamedCollection(const NamedCollection & collection, ContextPtr context)
{
    validateNamedCollection(collection, required_configuration_keys, optional_configuration_keys);

    String connection_url;
    String container_name;
    std::optional<String> account_name;
    std::optional<String> account_key;
    std::optional<String> client_id;
    std::optional<String> tenant_id;

    if (collection.has("connection_string"))
        connection_url = collection.get<String>("connection_string");
    else if (collection.has("storage_account_url"))
        connection_url = collection.get<String>("storage_account_url");

    container_name = collection.get<String>("container");
    blob_path = collection.get<String>("blob_path");

    if (collection.has("account_name"))
        account_name = collection.get<String>("account_name");

    if (collection.has("account_key"))
        account_key = collection.get<String>("account_key");

    if (collection.has("client_id"))
        client_id = collection.get<String>("client_id");

    if (collection.has("tenant_id"))
        tenant_id = collection.get<String>("tenant_id");

    structure = collection.getOrDefault<String>("structure", "auto");
    format = collection.getOrDefault<String>("format", format);
    compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));

    if (collection.has("partition_strategy"))
    {
        const auto partition_strategy_name = collection.get<std::string>("partition_strategy");
        const auto partition_strategy_type_opt = magic_enum::enum_cast<PartitionStrategyFactory::StrategyType>(partition_strategy_name, magic_enum::case_insensitive);

        if (!partition_strategy_type_opt)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition strategy {} is not supported", partition_strategy_name);
        }

        partition_strategy_type = partition_strategy_type_opt.value();
    }

    partition_columns_in_data_file = collection.getOrDefault<bool>("partition_columns_in_data_file", partition_strategy_type != PartitionStrategyFactory::StrategyType::HIVE);

    blobs_paths = {blob_path};
    connection_params = getConnectionParams(connection_url, container_name, account_name, account_key, client_id, tenant_id, context);
}

ASTPtr StorageAzureConfiguration::extractExtraCredentials(ASTs & args)
{
    for (size_t i = 0; i != args.size(); ++i)
    {
        const auto * ast_function = args[i]->as<ASTFunction>();
        if (ast_function && ast_function->name == "extra_credentials")
        {
            auto credentials = args[i];
            args.erase(args.begin() + i);
            return credentials;
        }
    }
    return nullptr;
}

bool StorageAzureConfiguration::collectCredentials(ASTPtr maybe_credentials, std::optional<String> & client_id, std::optional<String> & tenant_id, ContextPtr local_context)
{
    if (!maybe_credentials)
        return false;

    client_id = {};
    tenant_id = {};

    const auto * credentials_ast_function = maybe_credentials->as<ASTFunction>();
    if (!credentials_ast_function || credentials_ast_function->name != "extra_credentials")
        return false;

    const auto * credentials_function_args_expr = assert_cast<const ASTExpressionList *>(credentials_ast_function->arguments.get());
    auto credentials_function_args = credentials_function_args_expr->children;

    for (auto & credential_arg : credentials_function_args)
    {
        const auto * credential_ast = credential_arg->as<ASTFunction>();
        if (!credential_ast || credential_ast->name != "equals")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Credentials argument is incorrect");

        auto * credential_args_expr = assert_cast<ASTExpressionList *>(credential_ast->arguments.get());
        auto & credential_args = credential_args_expr->children;
        if (credential_args.size() != 2)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Credentials argument is incorrect: expected 2 arguments, got {}",
                credential_args.size());

        credential_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(credential_args[0], local_context);
        auto arg_name_value = credential_args[0]->as<ASTLiteral>()->value;
        if (arg_name_value.getType() != Field::Types::Which::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as credential name");
        auto arg_name = arg_name_value.safeGet<String>();

        credential_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(credential_args[1], local_context);
        auto arg_value = credential_args[1]->as<ASTLiteral>()->value;
        if (arg_value.getType() != Field::Types::Which::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as credential value");
        else if (arg_name == "client_id")
            client_id = arg_value.safeGet<String>();
        else if (arg_name == "tenant_id")
            tenant_id = arg_value.safeGet<String>();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid credential argument found: {}", arg_name);
    }

    return true;
}

void StorageAzureConfiguration::fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure)
{
    disk = context->getDisk(disk_name);
    const auto & azure_object_storage = assert_cast<const AzureObjectStorage &>(*disk->getObjectStorage());

    connection_params = azure_object_storage.getConnectionParameters();
    ParseFromDiskResult parsing_result = parseFromDisk(args, with_structure, context, disk->getPath());

    blob_path = "/" + parsing_result.path_suffix;
    setPathForRead(blob_path.path + "/");
    setPaths({blob_path.path + "/"});

    blobs_paths = {blob_path};
    if (parsing_result.format.has_value())
        format = *parsing_result.format;
    if (parsing_result.compression_method.has_value())
        compression_method = *parsing_result.compression_method;
    if (parsing_result.structure.has_value())
        structure = *parsing_result.structure;
}

void StorageAzureConfiguration::initializeForOneLake(ASTs & args, ContextPtr context)
{
    if (args.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only one argument should be provided in OneLake catalog");

    String connection_url = checkAndGetLiteralArgument<String>(args[0], "connection_string/storage_account_url");

    fillBlobsFromURLCommon(connection_url, ".com", ".dfs.fabric.microsoft.com");
    connection_params.endpoint.additional_params = "resource=REDACTED&directory=REDACTED&recursive=REDACTED";

    connection_params.auth_method = std::make_shared<Azure::Identity::ClientSecretCredential>(
        onelake_tenant_id,
        onelake_client_id,
        onelake_client_secret
    );

    auto request_settings = AzureBlobStorage::getRequestSettings(context->getSettingsRef());
    connection_params.client_options = AzureBlobStorage::getClientOptions(context, context->getSettingsRef(), *request_settings, /*for_disk=*/ false);
}

void StorageAzureConfiguration::fromAST(ASTs & engine_args, ContextPtr context, bool with_structure)
{
    if (!onelake_client_id.empty())
    {
        initializeForOneLake(engine_args, context);
        return;
    }

    auto extra_credentials = extractExtraCredentials(engine_args);

    if (engine_args.empty() || engine_args.size() > getMaxNumberOfArguments(with_structure))
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage AzureBlobStorage requires 1 to {} arguments. All supported signatures:\n{}",
            getMaxNumberOfArguments(with_structure),
            getSignatures(with_structure));
    }

    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

    /// This is only for lightweight loading of tables, so does not contain credentials
    /// for listing tables of Unity Catalog
    if (engine_args.size() == 1)
    {
        connection_params.endpoint.storage_account_url = checkAndGetLiteralArgument<String>(engine_args[0], "connection_string/storage_account_url");
        connection_params.endpoint.container_already_exists = true;
        return;
    }

    if (engine_args.size() == 2)
    {
        String connection_url = checkAndGetLiteralArgument<String>(engine_args[0], "connection_string/storage_account_url");
        String sas_token = checkAndGetLiteralArgument<String>(engine_args[1], "sas_token");
        fillBlobsFromURLCommon(connection_url, ".net", ".blob.core.windows.net");
        connection_params.endpoint.sas_auth = sas_token;

        return;
    }

    std::unordered_map<std::string_view, size_t> engine_args_to_idx;


    String connection_url = checkAndGetLiteralArgument<String>(engine_args[0], "connection_string/storage_account_url");
    String container_name = checkAndGetLiteralArgument<String>(engine_args[1], "container");
    blob_path = checkAndGetLiteralArgument<String>(engine_args[2], "blobpath");

    std::optional<String> account_name;
    std::optional<String> account_key;
    std::optional<String> client_id;
    std::optional<String> tenant_id;

    collectCredentials(extra_credentials, client_id, tenant_id, context);

    auto is_format_arg = [] (const std::string & s) -> bool
    {
        return s == "auto" || FormatFactory::instance().getAllFormats().contains(Poco::toLower(s));
    };

    if (engine_args.size() == 4)
    {
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
        if (is_format_arg(fourth_arg))
        {
            format = fourth_arg;
        }
        else
        {
            if (with_structure)
                structure = fourth_arg;
            else
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Unknown format or account name specified without account key: {}", fourth_arg);
        }
    }
    else if (engine_args.size() == 5)
    {
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
        if (is_format_arg(fourth_arg))
        {
            format = fourth_arg;
            compression_method = checkAndGetLiteralArgument<String>(engine_args[4], "compression");
        }
        else
        {
            account_name = fourth_arg;
            account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
        }
    }
    else if (engine_args.size() == 6)
    {
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
        if (is_format_arg(fourth_arg))
        {
            format = fourth_arg;
            compression_method = checkAndGetLiteralArgument<String>(engine_args[4], "compression");

            auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "partition_strategy/structure");
            if (magic_enum::enum_contains<PartitionStrategyFactory::StrategyType>(sixth_arg, magic_enum::case_insensitive))
            {
                partition_strategy_type = magic_enum::enum_cast<PartitionStrategyFactory::StrategyType>(sixth_arg, magic_enum::case_insensitive).value();
            }
            else
            {
                if (with_structure)
                {
                    structure = sixth_arg;
                }
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown partition strategy {}", sixth_arg);
            }
        }
        else
        {
            account_name = fourth_arg;
            account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
            auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format/structure");
            if (is_format_arg(sixth_arg))
            {
                format = sixth_arg;
            }
            else
            {
                if (with_structure)
                    structure = sixth_arg;
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
            }
        }
    }
    else if (engine_args.size() == 7)
    {
        const auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");

        if (is_format_arg(fourth_arg))
        {
            format = fourth_arg;
            compression_method = checkAndGetLiteralArgument<String>(engine_args[4], "compression");
            const auto partition_strategy_name = checkAndGetLiteralArgument<String>(engine_args[5], "partition_strategy");
            const auto partition_strategy_type_opt = magic_enum::enum_cast<PartitionStrategyFactory::StrategyType>(partition_strategy_name, magic_enum::case_insensitive);

            if (!partition_strategy_type_opt)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown partition strategy {}", partition_strategy_name);
            }

            partition_strategy_type = partition_strategy_type_opt.value();

            /// If it's of type String, then it is not `partition_columns_in_data_file`
            if (const auto seventh_arg = tryGetLiteralArgument<String>(engine_args[6], "structure/partition_columns_in_data_file"))
            {
                if (with_structure)
                {
                    structure = seventh_arg.value();
                }
                else
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected `partition_columns_in_data_file` of type boolean, but found: {}", seventh_arg.value());
                }
            }
            else
            {
                partition_columns_in_data_file = checkAndGetLiteralArgument<bool>(engine_args[6], "partition_columns_in_data_file");
            }
        }
        else
        {
            if (!with_structure && is_format_arg(fourth_arg))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Format and compression must be last arguments");
            }

            account_name = fourth_arg;
            account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
            auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format/account_name");
            if (!is_format_arg(sixth_arg))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
            format = sixth_arg;
            compression_method = checkAndGetLiteralArgument<String>(engine_args[6], "compression");
        }
    }
    else if (engine_args.size() == 8)
    {
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");

        if (is_format_arg(fourth_arg))
        {
            if (!with_structure)
            {
                /// If the fourth argument is a format, then it means a connection string is being used.
                /// When using a connection string, the function only accepts 8 arguments in case `with_structure=true`
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid sequence / combination of arguments");
            }
            format = fourth_arg;
            compression_method = checkAndGetLiteralArgument<String>(engine_args[4], "compression");
            const auto partition_strategy_name = checkAndGetLiteralArgument<String>(engine_args[5], "partition_strategy");
            const auto partition_strategy_type_opt = magic_enum::enum_cast<PartitionStrategyFactory::StrategyType>(partition_strategy_name, magic_enum::case_insensitive);

            if (!partition_strategy_type_opt)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown partition strategy {}", partition_strategy_name);
            }

            partition_strategy_type = partition_strategy_type_opt.value();
            partition_columns_in_data_file = checkAndGetLiteralArgument<bool>(engine_args[6], "partition_columns_in_data_file");
            structure = checkAndGetLiteralArgument<String>(engine_args[7], "structure");
        }
        else
        {
            account_name = fourth_arg;
            account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
            auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format");
            if (!is_format_arg(sixth_arg))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
            format = sixth_arg;
            compression_method = checkAndGetLiteralArgument<String>(engine_args[6], "compression");

            auto eighth_arg = checkAndGetLiteralArgument<String>(engine_args[7], "partition_strategy/structure");
            if (magic_enum::enum_contains<PartitionStrategyFactory::StrategyType>(eighth_arg, magic_enum::case_insensitive))
            {
                partition_strategy_type = magic_enum::enum_cast<PartitionStrategyFactory::StrategyType>(eighth_arg, magic_enum::case_insensitive).value();
            }
            else
            {
                if (with_structure)
                {
                    structure = eighth_arg;
                }
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown partition strategy {}", eighth_arg);
            }
        }
    }
    else if (engine_args.size() == 9)
    {
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
        account_name = fourth_arg;
        account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
        auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format");
        if (!is_format_arg(sixth_arg))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
        format = sixth_arg;
        compression_method = checkAndGetLiteralArgument<String>(engine_args[6], "compression");

        const auto partition_strategy_name = checkAndGetLiteralArgument<String>(engine_args[7], "partition_strategy");
        const auto partition_strategy_type_opt = magic_enum::enum_cast<PartitionStrategyFactory::StrategyType>(partition_strategy_name, magic_enum::case_insensitive);

        if (!partition_strategy_type_opt)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown partition strategy {}", partition_strategy_name);
        }
        partition_strategy_type = partition_strategy_type_opt.value();
        /// If it's of type String, then it is not `partition_columns_in_data_file`
        if (const auto nineth_arg = tryGetLiteralArgument<String>(engine_args[8], "structure/partition_columns_in_data_file"))
        {
            if (with_structure)
            {
                structure = nineth_arg.value();
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected `partition_columns_in_data_file` of type boolean, but found: {}", nineth_arg.value());
            }
        }
        else
        {
            partition_columns_in_data_file = checkAndGetLiteralArgument<bool>(engine_args[8], "partition_columns_in_data_file");
        }
    }
    else if (engine_args.size() == 10 && with_structure)
    {
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
        account_name = fourth_arg;
        account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
        auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format");
        if (!is_format_arg(sixth_arg))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
        format = sixth_arg;
        compression_method = checkAndGetLiteralArgument<String>(engine_args[6], "compression");

        const auto partition_strategy_name = checkAndGetLiteralArgument<String>(engine_args[7], "partition_strategy");
        const auto partition_strategy_type_opt = magic_enum::enum_cast<PartitionStrategyFactory::StrategyType>(partition_strategy_name, magic_enum::case_insensitive);

        if (!partition_strategy_type_opt)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown partition strategy {}", partition_strategy_name);
        }
        partition_strategy_type = partition_strategy_type_opt.value();
        partition_columns_in_data_file = checkAndGetLiteralArgument<bool>(engine_args[8], "partition_columns_in_data_file");
        structure = checkAndGetLiteralArgument<String>(engine_args[9], "structure");
    }

    blobs_paths = {blob_path};
    connection_params = getConnectionParams(connection_url, container_name, account_name, account_key, client_id, tenant_id, context);
}

void StorageAzureConfiguration::addStructureAndFormatToArgsIfNeeded(
    ASTs & args, const String & structure_, const String & format_, ContextPtr context, bool with_structure)
{
    if (disk)
    {
        if (format == "auto")
        {
            ASTs format_equal_func_args = {std::make_shared<ASTIdentifier>("format"), std::make_shared<ASTLiteral>(format_)};
            auto format_equal_func = makeASTFunction("equals", std::move(format_equal_func_args));
            args.push_back(format_equal_func);
        }
        if (structure == "auto")
        {
            ASTs structure_equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure_)};
            auto structure_equal_func = makeASTFunction("equals", std::move(structure_equal_func_args));
            args.push_back(structure_equal_func);
        }
    }
    else if (auto collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pairs "format='...', structure='...'"
        /// at the end of arguments to override existed format and structure with "auto" values.
        if (collection->getOrDefault<String>("format", "auto") == "auto")
        {
            ASTs format_equal_func_args = {std::make_shared<ASTIdentifier>("format"), std::make_shared<ASTLiteral>(format_)};
            auto format_equal_func = makeASTOperator("equals", std::move(format_equal_func_args));
            args.push_back(format_equal_func);
        }
        if (with_structure && collection->getOrDefault<String>("structure", "auto") == "auto")
        {
            ASTs structure_equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure_)};
            auto structure_equal_func = makeASTOperator("equals", std::move(structure_equal_func_args));
            args.push_back(structure_equal_func);
        }
    }
    else
    {
        if (args.size() < 3 || args.size() > getMaxNumberOfArguments())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected 3 to {} arguments in table function azureBlobStorage, got {}", getMaxNumberOfArguments(), args.size());

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        auto structure_literal = std::make_shared<ASTLiteral>(structure_);
        auto format_literal = std::make_shared<ASTLiteral>(format_);
        auto is_format_arg = [] (const std::string & s) -> bool
        {
            return s == "auto" || FormatFactory::instance().getAllFormats().contains(Poco::toLower(s));
        };

        /// (connection_string, container_name, blobpath)
        if (args.size() == 3)
        {
            args.push_back(format_literal);
            if (with_structure)
            {
                /// Add compression = "auto" before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(structure_literal);
            }
        }
        /// (connection_string, container_name, blobpath, structure) or
        /// (connection_string, container_name, blobpath, format)
        /// We can distinguish them by looking at the 4-th argument: check if it's format name or not.
        else if (args.size() == 4)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/account_name/structure");
            /// (..., format) -> (..., format, compression, structure)
            if (is_format_arg(fourth_arg))
            {
                if (fourth_arg == "auto")
                    args[3] = format_literal;
                if (with_structure)
                {
                    /// Add compression=auto before structure argument.
                    args.push_back(std::make_shared<ASTLiteral>("auto"));
                    args.push_back(structure_literal);
                }
            }
            /// (..., structure) -> (..., format, compression, structure)
            else if (with_structure)
            {
                auto structure_arg = args.back();
                args[3] = format_literal;
                /// Add compression=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                if (fourth_arg == "auto")
                    args.push_back(structure_literal);
                else
                    args.push_back(structure_arg);
            }
        }
        /// (connection_string, container_name, blobpath, format, compression) or
        /// (storage_account_url, container_name, blobpath, account_name, account_key)
        /// We can distinguish them by looking at the 4-th argument: check if it's format name or not.
        else if (args.size() == 5)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/account_name");
            /// (..., format, compression) -> (..., format, compression, structure)
            if (is_format_arg(fourth_arg))
            {
                if (fourth_arg == "auto")
                    args[3] = format_literal;
                if (with_structure)
                    args.push_back(structure_literal);
            }
            /// (..., account_name, account_key) -> (..., account_name, account_key, format, compression, structure)
            else
            {
                args.push_back(format_literal);
                if (with_structure)
                {
                    /// Add compression=auto before structure argument.
                    args.push_back(std::make_shared<ASTLiteral>("auto"));
                    args.push_back(structure_literal);
                }
            }
        }
        /// (connection_string, container_name, blobpath, format, compression, structure) or
        /// (storage_account_url, container_name, blobpath, account_name, account_key, structure) or
        /// (storage_account_url, container_name, blobpath, account_name, account_key, format)
        else if (args.size() == 6)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/account_name");
            auto sixth_arg = checkAndGetLiteralArgument<String>(args[5], "format/structure");

            /// (..., format, compression, structure)
            if (is_format_arg(fourth_arg))
            {
                if (fourth_arg == "auto")
                    args[3] = format_literal;
                if (with_structure && checkAndGetLiteralArgument<String>(args[5], "structure") == "auto")
                    args[5] = structure_literal;
            }
            /// (..., account_name, account_key, format) -> (..., account_name, account_key, format, compression, structure)
            else if (is_format_arg(sixth_arg))
            {
                if (sixth_arg == "auto")
                    args[5] = format_literal;
                if (with_structure)
                {
                    /// Add compression=auto before structure argument.
                    args.push_back(std::make_shared<ASTLiteral>("auto"));
                    args.push_back(structure_literal);
                }
            }
            /// (..., account_name, account_key, structure) -> (..., account_name, account_key, format, compression, structure)
            else if (with_structure)
            {
                auto structure_arg = args.back();
                args[5] = format_literal;
                /// Add compression=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                if (sixth_arg == "auto")
                    args.push_back(structure_literal);
                else
                    args.push_back(structure_arg);
            }
        }
        /// (storage_account_url, container_name, blobpath, account_name, account_key, format, compression)
        else if (args.size() == 7)
        {
            /// (..., format, compression) -> (..., format, compression, structure)
            if (checkAndGetLiteralArgument<String>(args[5], "format") == "auto")
                args[5] = format_literal;
            if (with_structure)
                args.push_back(structure_literal);
        }
        /// (storage_account_url, container_name, blobpath, account_name, account_key, format, compression, structure)
        else if (args.size() == 8)
        {
            if (checkAndGetLiteralArgument<String>(args[5], "format") == "auto")
                args[5] = format_literal;
            if (with_structure && checkAndGetLiteralArgument<String>(args[7], "structure") == "auto")
                args[7] = structure_literal;
        }
    }
}

}

#endif
