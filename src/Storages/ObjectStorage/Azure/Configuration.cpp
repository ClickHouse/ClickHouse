#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Poco/URI.h>

#if USE_AZURE_BLOB_STORAGE

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
#include <Core/Settings.h>
#include <Common/RemoteHostFilter.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

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
};

void StorageAzureConfiguration::check(ContextPtr context) const
{
    auto url = Poco::URI(connection_params.getConnectionURL());
    context->getGlobalContext()->getRemoteHostFilter().checkURL(url);
    Configuration::check(context);
}

StorageObjectStorage::QuerySettings StorageAzureConfiguration::getQuerySettings(const ContextPtr & context) const
{
    const auto & settings = context->getSettingsRef();
    return StorageObjectStorage::QuerySettings{
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
        connection_params.getContainer(),
        connection_params.getConnectionURL());
}

static AzureBlobStorage::ConnectionParams getConnectionParams(
    const String & connection_url,
    const String & container_name,
    const std::optional<String> & account_name,
    const std::optional<String> & account_key,
    const ContextPtr & local_context)
{
    AzureBlobStorage::ConnectionParams connection_params;
    auto request_settings = AzureBlobStorage::getRequestSettings(local_context->getSettingsRef());

    if (account_name && account_key)
    {
        connection_params.endpoint.storage_account_url = connection_url;
        connection_params.endpoint.container_name = container_name;
        connection_params.auth_method = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(*account_name, *account_key);
        connection_params.client_options = AzureBlobStorage::getClientOptions(*request_settings, /*for_disk=*/ false);
    }
    else
    {
        AzureBlobStorage::processURL(connection_url, container_name, connection_params.endpoint, connection_params.auth_method);
        connection_params.client_options = AzureBlobStorage::getClientOptions(*request_settings, /*for_disk=*/ false);
    }

    return connection_params;
}

void StorageAzureConfiguration::fromNamedCollection(const NamedCollection & collection, ContextPtr context)
{
    validateNamedCollection(collection, required_configuration_keys, optional_configuration_keys);

    String connection_url;
    String container_name;
    std::optional<String> account_name;
    std::optional<String> account_key;

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

    structure = collection.getOrDefault<String>("structure", "auto");
    format = collection.getOrDefault<String>("format", format);
    compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));

    blobs_paths = {blob_path};
    connection_params = getConnectionParams(connection_url, container_name, account_name, account_key, context);
}

void StorageAzureConfiguration::fromAST(ASTs & engine_args, ContextPtr context, bool with_structure)
{
    if (engine_args.size() < 3 || engine_args.size() > getMaxNumberOfArguments(with_structure))
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage AzureBlobStorage requires 3 to {} arguments. All supported signatures:\n{}",
            getMaxNumberOfArguments(with_structure),
            getSignatures(with_structure));
    }

    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

    std::unordered_map<std::string_view, size_t> engine_args_to_idx;


    String connection_url = checkAndGetLiteralArgument<String>(engine_args[0], "connection_string/storage_account_url");
    String container_name = checkAndGetLiteralArgument<String>(engine_args[1], "container");
    blob_path = checkAndGetLiteralArgument<String>(engine_args[2], "blobpath");

    std::optional<String> account_name;
    std::optional<String> account_key;

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
            if (with_structure)
            {
                format = fourth_arg;
                compression_method = checkAndGetLiteralArgument<String>(engine_args[4], "compression");
                structure = checkAndGetLiteralArgument<String>(engine_args[5], "structure");
            }
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Format and compression must be last arguments");
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
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
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
    else if (with_structure && engine_args.size() == 8)
    {
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "account_name");
        account_name = fourth_arg;
        account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
        auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format");
        if (!is_format_arg(sixth_arg))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
        format = sixth_arg;
        compression_method = checkAndGetLiteralArgument<String>(engine_args[6], "compression");
        structure = checkAndGetLiteralArgument<String>(engine_args[7], "structure");
    }

    blobs_paths = {blob_path};
    connection_params = getConnectionParams(connection_url, container_name, account_name, account_key, context);
}

void StorageAzureConfiguration::addStructureAndFormatToArgsIfNeeded(
    ASTs & args, const String & structure_, const String & format_, ContextPtr context, bool with_structure)
{
    if (auto collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pairs "format='...', structure='...'"
        /// at the end of arguments to override existed format and structure with "auto" values.
        if (collection->getOrDefault<String>("format", "auto") == "auto")
        {
            ASTs format_equal_func_args = {std::make_shared<ASTIdentifier>("format"), std::make_shared<ASTLiteral>(format_)};
            auto format_equal_func = makeASTFunction("equals", std::move(format_equal_func_args));
            args.push_back(format_equal_func);
        }
        if (with_structure && collection->getOrDefault<String>("structure", "auto") == "auto")
        {
            ASTs structure_equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure_)};
            auto structure_equal_func = makeASTFunction("equals", std::move(structure_equal_func_args));
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
