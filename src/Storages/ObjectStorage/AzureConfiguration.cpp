#include <Storages/ObjectStorage/AzureConfiguration.h>
#include <azure/storage/common/storage_credential.hpp>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/IO/WriteBufferFromAzureBlobStorage.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Formats/FormatFactory.h>
#include <azure/storage/blobs.hpp>
#include <Interpreters/evaluateConstantExpression.h>
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

    bool containerExists(std::unique_ptr<Azure::Storage::Blobs::BlobServiceClient> & blob_service_client, std::string container_name)
    {
        Azure::Storage::Blobs::ListBlobContainersOptions options;
        options.Prefix = container_name;
        options.PageSizeHint = 1;

        auto containers_list_response = blob_service_client->ListBlobContainers(options);
        auto containers_list = containers_list_response.BlobContainers;

        for (const auto & container : containers_list)
        {
            if (container_name == container.Name)
                return true;
        }
        return false;
    }
}

void StorageAzureBlobConfiguration::check(ContextPtr context) const
{
    Poco::URI url_to_check;
    if (is_connection_string)
    {
        auto parsed_connection_string = Azure::Storage::_internal::ParseConnectionString(connection_url);
        url_to_check = Poco::URI(parsed_connection_string.BlobServiceUrl.GetAbsoluteUrl());
    }
    else
        url_to_check = Poco::URI(connection_url);

    context->getGlobalContext()->getRemoteHostFilter().checkURL(url_to_check);
}

StorageObjectStorageConfigurationPtr StorageAzureBlobConfiguration::clone()
{
    auto configuration = std::make_shared<StorageAzureBlobConfiguration>();
    configuration->connection_url = connection_url;
    configuration->is_connection_string = is_connection_string;
    configuration->account_name = account_name;
    configuration->account_key = account_key;
    configuration->container = container;
    configuration->blob_path = blob_path;
    configuration->blobs_paths = blobs_paths;
    return configuration;
}

AzureObjectStorage::SettingsPtr StorageAzureBlobConfiguration::createSettings(ContextPtr context)
{
    const auto & context_settings = context->getSettingsRef();
    auto settings_ptr = std::make_unique<AzureObjectStorageSettings>();
    settings_ptr->max_single_part_upload_size = context_settings.azure_max_single_part_upload_size;
    settings_ptr->max_single_read_retries = context_settings.azure_max_single_read_retries;
    settings_ptr->list_object_keys_size = static_cast<int32_t>(context_settings.azure_list_object_keys_size);
    return settings_ptr;
}

ObjectStoragePtr StorageAzureBlobConfiguration::createOrUpdateObjectStorage(ContextPtr context, bool is_readonly) /// NOLINT
{
    auto client = createClient(is_readonly);
    auto settings = createSettings(context);
    return std::make_unique<AzureObjectStorage>("AzureBlobStorage", std::move(client), std::move(settings), container);
}

AzureClientPtr StorageAzureBlobConfiguration::createClient(bool is_read_only)
{
    using namespace Azure::Storage::Blobs;

    AzureClientPtr result;

    if (is_connection_string)
    {
        auto blob_service_client = std::make_unique<BlobServiceClient>(BlobServiceClient::CreateFromConnectionString(connection_url));
        result = std::make_unique<BlobContainerClient>(BlobContainerClient::CreateFromConnectionString(connection_url, container));
        bool container_exists = containerExists(blob_service_client, container);

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
            } catch (const Azure::Storage::StorageException & e)
            {
                if (!(e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict
                    && e.ReasonPhrase == "The specified container already exists."))
                {
                    throw;
                }
            }
        }
    }
    else
    {
        std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> storage_shared_key_credential;
        if (account_name.has_value() && account_key.has_value())
        {
            storage_shared_key_credential =
                std::make_shared<Azure::Storage::StorageSharedKeyCredential>(*account_name, *account_key);
        }

        std::unique_ptr<BlobServiceClient> blob_service_client;
        if (storage_shared_key_credential)
        {
            blob_service_client = std::make_unique<BlobServiceClient>(connection_url, storage_shared_key_credential);
        }
        else
        {
            blob_service_client = std::make_unique<BlobServiceClient>(connection_url);
        }

        bool container_exists = containerExists(blob_service_client, container);

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

        if (container_exists)
        {
            if (storage_shared_key_credential)
                result = std::make_unique<BlobContainerClient>(final_url, storage_shared_key_credential);
            else
                result = std::make_unique<BlobContainerClient>(final_url);
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
            }
            catch (const Azure::Storage::StorageException & e)
            {
                if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict
                      && e.ReasonPhrase == "The specified container already exists.")
                {
                    if (storage_shared_key_credential)
                        result = std::make_unique<BlobContainerClient>(final_url, storage_shared_key_credential);
                    else
                        result = std::make_unique<BlobContainerClient>(final_url);
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

void StorageAzureBlobConfiguration::fromNamedCollection(const NamedCollection & collection)
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
    if (format == "auto")
        format = FormatFactory::instance().getFormatFromFileName(blob_path, true);
}

void StorageAzureBlobConfiguration::fromAST(ASTs & engine_args, ContextPtr context, bool with_structure)
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
        return s == "auto" || FormatFactory::instance().getAllFormats().contains(s);
    };

    if (engine_args.size() == 4)
    {
        //'c1 UInt64, c2 UInt64
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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format or account name specified without account key");
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

    if (format == "auto")
        format = FormatFactory::instance().getFormatFromFileName(blob_path, true);
}

void StorageAzureBlobConfiguration::addStructureToArgs(ASTs & args, const String & structure_, ContextPtr context)
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
                            "StorageObjectStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])");
        }

        auto structure_literal = std::make_shared<ASTLiteral>(structure_);
        auto is_format_arg
            = [](const std::string & s) -> bool { return s == "auto" || FormatFactory::instance().getAllFormats().contains(s); };

        if (args.size() == 3)
        {
            /// Add format=auto & compression=auto before structure argument.
            args.push_back(std::make_shared<ASTLiteral>("auto"));
            args.push_back(std::make_shared<ASTLiteral>("auto"));
            args.push_back(structure_literal);
        }
        else if (args.size() == 4)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/account_name/structure");
            if (is_format_arg(fourth_arg))
            {
                /// Add compression=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(structure_literal);
            }
            else
            {
                args.back() = structure_literal;
            }
        }
        else if (args.size() == 5)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/account_name");
            if (!is_format_arg(fourth_arg))
            {
                /// Add format=auto & compression=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(std::make_shared<ASTLiteral>("auto"));
            }
            args.push_back(structure_literal);
        }
        else if (args.size() == 6)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/account_name");
            if (!is_format_arg(fourth_arg))
            {
                /// Add compression=auto before structure argument.
                args.push_back(std::make_shared<ASTLiteral>("auto"));
                args.push_back(structure_literal);
            }
            else
            {
                args.back() = structure_literal;
            }
        }
        else if (args.size() == 7)
        {
            args.push_back(structure_literal);
        }
        else if (args.size() == 8)
        {
            args.back() = structure_literal;
        }
    }
}

}
