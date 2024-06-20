#include <Storages/ObjectStorage/Azure/Configuration.h>

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
#include <azure/identity/managed_identity_credential.hpp>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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

using AzureClient = Azure::Storage::Blobs::BlobContainerClient;
using AzureClientPtr = std::unique_ptr<Azure::Storage::Blobs::BlobContainerClient>;

namespace
{
    bool isConnectionString(const std::string & candidate)
    {
        return !candidate.starts_with("http");
    }

    template <typename T>
    bool containerExists(T & blob_service_client, const std::string & container_name)
    {
        Azure::Storage::Blobs::ListBlobContainersOptions options;
        options.Prefix = container_name;
        options.PageSizeHint = 1;

        auto containers_list_response = blob_service_client.ListBlobContainers(options);
        auto containers_list = containers_list_response.BlobContainers;

        auto it = std::find_if(
            containers_list.begin(), containers_list.end(),
            [&](const auto & c) { return c.Name == container_name; });
        return it != containers_list.end();
    }
}

Poco::URI StorageAzureConfiguration::getConnectionURL() const
{
    if (!is_connection_string)
        return Poco::URI(connection_url);

    auto parsed_connection_string = Azure::Storage::_internal::ParseConnectionString(connection_url);
    return Poco::URI(parsed_connection_string.BlobServiceUrl.GetAbsoluteUrl());
}

void StorageAzureConfiguration::check(ContextPtr context) const
{
    context->getGlobalContext()->getRemoteHostFilter().checkURL(getConnectionURL());
    Configuration::check(context);
}

StorageAzureConfiguration::StorageAzureConfiguration(const StorageAzureConfiguration & other)
    : Configuration(other)
{
    connection_url = other.connection_url;
    is_connection_string = other.is_connection_string;
    account_name = other.account_name;
    account_key = other.account_key;
    container = other.container;
    blob_path = other.blob_path;
    blobs_paths = other.blobs_paths;
}

AzureObjectStorage::SettingsPtr StorageAzureConfiguration::createSettings(ContextPtr context)
{
    const auto & context_settings = context->getSettingsRef();
    auto settings_ptr = std::make_unique<AzureObjectStorageSettings>();
    settings_ptr->max_single_part_upload_size = context_settings.azure_max_single_part_upload_size;
    settings_ptr->max_single_read_retries = context_settings.azure_max_single_read_retries;
    settings_ptr->list_object_keys_size = static_cast<int32_t>(context_settings.azure_list_object_keys_size);
    settings_ptr->strict_upload_part_size = context_settings.azure_strict_upload_part_size;
    settings_ptr->max_upload_part_size = context_settings.azure_max_upload_part_size;
    settings_ptr->max_blocks_in_multipart_upload = context_settings.azure_max_blocks_in_multipart_upload;
    settings_ptr->min_upload_part_size = context_settings.azure_min_upload_part_size;
    return settings_ptr;
}

StorageObjectStorage::QuerySettings StorageAzureConfiguration::getQuerySettings(const ContextPtr & context) const
{
    const auto & settings = context->getSettingsRef();
    return StorageObjectStorage::QuerySettings{
        .truncate_on_insert = settings.azure_truncate_on_insert,
        .create_new_file_on_insert = settings.azure_create_new_file_on_insert,
        .schema_inference_use_cache = settings.schema_inference_use_cache_for_azure,
        .schema_inference_mode = settings.schema_inference_mode,
        .skip_empty_files = settings.azure_skip_empty_files,
        .list_object_keys_size = settings.azure_list_object_keys_size,
        .throw_on_zero_files_match = settings.azure_throw_on_zero_files_match,
        .ignore_non_existent_file = settings.azure_ignore_file_doesnt_exist,
    };
}

ObjectStoragePtr StorageAzureConfiguration::createObjectStorage(ContextPtr context, bool is_readonly) /// NOLINT
{
    assertInitialized();
    auto client = createClient(is_readonly, /* attempt_to_create_container */true);
    auto settings = createSettings(context);
    return std::make_unique<AzureObjectStorage>(
        "AzureBlobStorage", std::move(client), std::move(settings), container, getConnectionURL().toString());
}

AzureClientPtr StorageAzureConfiguration::createClient(bool is_read_only, bool attempt_to_create_container)
{
    using namespace Azure::Storage::Blobs;

    AzureClientPtr result;

    if (is_connection_string)
    {
        auto managed_identity_credential = std::make_shared<Azure::Identity::ManagedIdentityCredential>();
        auto blob_service_client = std::make_unique<BlobServiceClient>(BlobServiceClient::CreateFromConnectionString(connection_url));
        result = std::make_unique<BlobContainerClient>(BlobContainerClient::CreateFromConnectionString(connection_url, container));

        if (attempt_to_create_container)
        {
            bool container_exists = containerExists(*blob_service_client, container);
            if (!container_exists)
            {
                if (is_read_only)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "AzureBlobStorage container does not exist '{}'",
                        container);

                try
                {
                    result->CreateIfNotExists();
                }
                catch (const Azure::Storage::StorageException & e)
                {
                    if (!(e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict
                        && e.ReasonPhrase == "The specified container already exists."))
                    {
                        throw;
                    }
                }
            }
        }
    }
    else
    {
        std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> storage_shared_key_credential;
        if (account_name.has_value() && account_key.has_value())
        {
            storage_shared_key_credential
                = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(*account_name, *account_key);
        }

        std::unique_ptr<BlobServiceClient> blob_service_client;
        std::shared_ptr<Azure::Identity::ManagedIdentityCredential> managed_identity_credential;
        if (storage_shared_key_credential)
        {
            blob_service_client = std::make_unique<BlobServiceClient>(connection_url, storage_shared_key_credential);
        }
        else
        {
            managed_identity_credential = std::make_shared<Azure::Identity::ManagedIdentityCredential>();
            blob_service_client = std::make_unique<BlobServiceClient>(connection_url, managed_identity_credential);
        }

        std::string final_url;
        size_t pos = connection_url.find('?');
        if (pos != std::string::npos)
        {
            auto url_without_sas = connection_url.substr(0, pos);
            final_url = url_without_sas + (url_without_sas.back() == '/' ? "" : "/") + container
                + connection_url.substr(pos);
        }
        else
            final_url
                = connection_url + (connection_url.back() == '/' ? "" : "/") + container;

        if (!attempt_to_create_container)
        {
            if (storage_shared_key_credential)
                return std::make_unique<BlobContainerClient>(final_url, storage_shared_key_credential);
            else
                return std::make_unique<BlobContainerClient>(final_url, managed_identity_credential);
        }

        bool container_exists = containerExists(*blob_service_client, container);
        if (container_exists)
        {
            if (storage_shared_key_credential)
                result = std::make_unique<BlobContainerClient>(final_url, storage_shared_key_credential);
            else
                result = std::make_unique<BlobContainerClient>(final_url, managed_identity_credential);
        }
        else
        {
            if (is_read_only)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "AzureBlobStorage container does not exist '{}'",
                    container);
            try
            {
                result = std::make_unique<BlobContainerClient>(blob_service_client->CreateBlobContainer(container).Value);
            } catch (const Azure::Storage::StorageException & e)
            {
                if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict
                      && e.ReasonPhrase == "The specified container already exists.")
                {
                    if (storage_shared_key_credential)
                        result = std::make_unique<BlobContainerClient>(final_url, storage_shared_key_credential);
                    else
                        result = std::make_unique<BlobContainerClient>(final_url, managed_identity_credential);
                }
                else
                {
                    throw;
                }
            }
        }
    }

    return result;
}

    void StorageAzureConfiguration::fromNamedCollection(const NamedCollection & collection, ContextPtr)
{
    validateNamedCollection(collection, required_configuration_keys, optional_configuration_keys);

    if (collection.has("connection_string"))
    {
        connection_url = collection.get<String>("connection_string");
        is_connection_string = true;
    }

    if (collection.has("storage_account_url"))
    {
        connection_url = collection.get<String>("storage_account_url");
        is_connection_string = false;
    }

    container = collection.get<String>("container");
    blob_path = collection.get<String>("blob_path");

    if (collection.has("account_name"))
        account_name = collection.get<String>("account_name");

    if (collection.has("account_key"))
        account_key = collection.get<String>("account_key");

    structure = collection.getOrDefault<String>("structure", "auto");
    format = collection.getOrDefault<String>("format", format);
    compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));

    blobs_paths = {blob_path};
}

void StorageAzureConfiguration::fromAST(ASTs & engine_args, ContextPtr context, bool with_structure)
{
    if (engine_args.size() < 3 || engine_args.size() > (with_structure ? 8 : 7))
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Storage AzureBlobStorage requires 3 to 7 arguments: "
                        "AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, "
                        "[account_name, account_key, format, compression, structure)])");
    }

    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

    std::unordered_map<std::string_view, size_t> engine_args_to_idx;

    connection_url = checkAndGetLiteralArgument<String>(engine_args[0], "connection_string/storage_account_url");
    is_connection_string = isConnectionString(connection_url);

    container = checkAndGetLiteralArgument<String>(engine_args[1], "container");
    blob_path = checkAndGetLiteralArgument<String>(engine_args[2], "blobpath");

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
            auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format/account_name");
            if (is_format_arg(sixth_arg))
                format = sixth_arg;
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
        else
        {
            account_name = fourth_arg;
            account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
            auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format/account_name");
            if (!is_format_arg(sixth_arg))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
            format = sixth_arg;
            compression_method = checkAndGetLiteralArgument<String>(engine_args[6], "compression");
        }
    }
    else if (with_structure && engine_args.size() == 8)
    {
        auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
        account_name = fourth_arg;
        account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
        auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format/account_name");
        if (!is_format_arg(sixth_arg))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
        format = sixth_arg;
        compression_method = checkAndGetLiteralArgument<String>(engine_args[6], "compression");
        structure = checkAndGetLiteralArgument<String>(engine_args[7], "structure");
    }

    blobs_paths = {blob_path};
}

void StorageAzureConfiguration::addStructureAndFormatToArgs(
    ASTs & args, const String & structure_, const String & format_, ContextPtr context)
{
    if (tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pair "structure='...'"
        /// at the end of arguments to override existed structure.
        ASTs equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure_)};
        auto equal_func = makeASTFunction("equals", std::move(equal_func_args));
        args.push_back(equal_func);
    }
    else
    {
        if (args.size() < 3 || args.size() > 8)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage Azure requires 3 to 7 arguments: "
                            "StorageObjectStorage(connection_string|storage_account_url, container_name, "
                            "blobpath, [account_name, account_key, format, compression, structure])");
        }

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        auto structure_literal = std::make_shared<ASTLiteral>(structure_);
        auto format_literal = std::make_shared<ASTLiteral>(format_);
        auto is_format_arg
            = [](const std::string & s) -> bool { return s == "auto" || FormatFactory::instance().getAllFormats().contains(s); };

        /// (connection_string, container_name, blobpath)
        if (args.size() == 3)
        {
            args.push_back(format_literal);
            /// Add compression = "auto" before structure argument.
            args.push_back(std::make_shared<ASTLiteral>("auto"));
            args.push_back(structure_literal);
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
                /// Add compression=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(structure_literal);
            }
            /// (..., structure) -> (..., format, compression, structure)
            else
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
                args.push_back(structure_literal);
            }
            /// (..., account_name, account_key) -> (..., account_name, account_key, format, compression, structure)
            else
            {
                args.push_back(format_literal);
                /// Add compression=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(structure_literal);
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
                if (checkAndGetLiteralArgument<String>(args[5], "structure") == "auto")
                    args[5] = structure_literal;
            }
            /// (..., account_name, account_key, format) -> (..., account_name, account_key, format, compression, structure)
            else if (is_format_arg(sixth_arg))
            {
                if (sixth_arg == "auto")
                    args[5] = format_literal;
                /// Add compression=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(structure_literal);
            }
            /// (..., account_name, account_key, structure) -> (..., account_name, account_key, format, compression, structure)
            else
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
            args.push_back(structure_literal);
        }
        /// (storage_account_url, container_name, blobpath, account_name, account_key, format, compression, structure)
        else if (args.size() == 8)
        {
            if (checkAndGetLiteralArgument<String>(args[5], "format") == "auto")
                args[5] = format_literal;
            if (checkAndGetLiteralArgument<String>(args[7], "structure") == "auto")
                args[7] = structure_literal;
        }
    }
}

}

#endif
