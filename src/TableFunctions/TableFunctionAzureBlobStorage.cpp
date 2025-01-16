#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/TableFunctionAzureBlobStorage.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageAzureBlob.h>
#include <Storages/StorageURL.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/VirtualColumnUtils.h>
#include <Formats/FormatFactory.h>
#include "registerTableFunctions.h"
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool isConnectionString(const std::string & candidate)
{
    return !candidate.starts_with("http");
}

}

void TableFunctionAzureBlobStorage::parseArgumentsImpl(ASTs & engine_args, const ContextPtr & local_context)
{
    /// Supported signatures:
    ///
    /// AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
    ///

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
    {
        StorageAzureBlob::processNamedCollectionResult(configuration, *named_collection);

        configuration.blobs_paths = {configuration.blob_path};

        if (configuration.format == "auto")
            configuration.format = FormatFactory::instance().tryGetFormatFromFileName(configuration.blob_path).value_or("auto");
    }
    else
    {
        if (engine_args.size() < 3 || engine_args.size() > 8)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage Azure requires 3 to 7 arguments: "
                "AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])");

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, local_context);

        std::unordered_map<std::string_view, size_t> engine_args_to_idx;

        configuration.connection_url = checkAndGetLiteralArgument<String>(engine_args[0], "connection_string/storage_account_url");
        configuration.is_connection_string = isConnectionString(configuration.connection_url);

        configuration.container = checkAndGetLiteralArgument<String>(engine_args[1], "container");
        configuration.blob_path = checkAndGetLiteralArgument<String>(engine_args[2], "blobpath");

        auto is_format_arg
            = [](const std::string & s) -> bool { return s == "auto" || FormatFactory::instance().exists(s); };

        if (engine_args.size() == 4)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name/structure");
            if (is_format_arg(fourth_arg))
            {
                configuration.format = fourth_arg;
            }
            else
            {
                configuration.structure = fourth_arg;
            }
        }
        else if (engine_args.size() == 5)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
            if (is_format_arg(fourth_arg))
            {
                configuration.format = fourth_arg;
                configuration.compression_method = checkAndGetLiteralArgument<String>(engine_args[4], "compression");
            }
            else
            {
                configuration.account_name = fourth_arg;
                configuration.account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
            }
        }
        else if (engine_args.size() == 6)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
            if (is_format_arg(fourth_arg))
            {
                configuration.format = fourth_arg;
                configuration.compression_method = checkAndGetLiteralArgument<String>(engine_args[4], "compression");
                configuration.structure = checkAndGetLiteralArgument<String>(engine_args[5], "structure");
            }
            else
            {
                configuration.account_name = fourth_arg;
                configuration.account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
                auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format/account_name/structure");
                if (is_format_arg(sixth_arg))
                    configuration.format = sixth_arg;
                else
                    configuration.structure = sixth_arg;
            }
        }
        else if (engine_args.size() == 7)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
            configuration.account_name = fourth_arg;
            configuration.account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
            auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format/account_name");
            if (!is_format_arg(sixth_arg))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
            configuration.format = sixth_arg;
            configuration.compression_method = checkAndGetLiteralArgument<String>(engine_args[6], "compression");
        }
        else if (engine_args.size() == 8)
        {
            auto fourth_arg = checkAndGetLiteralArgument<String>(engine_args[3], "format/account_name");
            configuration.account_name = fourth_arg;
            configuration.account_key = checkAndGetLiteralArgument<String>(engine_args[4], "account_key");
            auto sixth_arg = checkAndGetLiteralArgument<String>(engine_args[5], "format/account_name");
            if (!is_format_arg(sixth_arg))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown format {}", sixth_arg);
            configuration.format = sixth_arg;
            configuration.compression_method = checkAndGetLiteralArgument<String>(engine_args[6], "compression");
            configuration.structure = checkAndGetLiteralArgument<String>(engine_args[7], "structure");
        }

        configuration.blobs_paths = {configuration.blob_path};

        if (configuration.format == "auto")
            configuration.format = FormatFactory::instance().tryGetFormatFromFileName(configuration.blob_path).value_or("auto");
    }
}

void TableFunctionAzureBlobStorage::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Clone ast function, because we can modify its arguments like removing headers.
    auto ast_copy = ast_function->clone();

    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    auto & args = args_func.at(0)->children;

    parseArgumentsImpl(args, context);
}

void TableFunctionAzureBlobStorage::updateStructureAndFormatArgumentsIfNeeded(ASTs & args, const String & structure, const String & format, const ContextPtr & context)
{
    if (auto collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pairs "format='...', structure='...'"
        /// at the end of arguments to override existed format and structure with "auto" values.
        if (collection->getOrDefault<String>("format", "auto") == "auto")
        {
            ASTs format_equal_func_args = {std::make_shared<ASTIdentifier>("format"), std::make_shared<ASTLiteral>(format)};
            auto format_equal_func = makeASTFunction("equals", std::move(format_equal_func_args));
            args.push_back(format_equal_func);
        }
        if (collection->getOrDefault<String>("structure", "auto") == "auto")
        {
            ASTs structure_equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure)};
            auto structure_equal_func = makeASTFunction("equals", std::move(structure_equal_func_args));
            args.push_back(structure_equal_func);
        }
    }
    else
    {
        if (args.size() < 3 || args.size() > 8)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage Azure requires 3 to 7 arguments: "
                            "AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])");

        auto format_literal = std::make_shared<ASTLiteral>(format);
        auto structure_literal = std::make_shared<ASTLiteral>(structure);

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        auto is_format_arg
            = [](const std::string & s) -> bool { return s == "auto" || FormatFactory::instance().exists(s); };

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

ColumnsDescription TableFunctionAzureBlobStorage::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    if (configuration.structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        auto client = StorageAzureBlob::createClient(configuration, !is_insert_query);
        auto settings = StorageAzureBlob::createSettings(context);

        auto object_storage = std::make_unique<AzureObjectStorage>("AzureBlobStorageTableFunction", std::move(client), std::move(settings), configuration.container);
        if (configuration.format == "auto")
            return StorageAzureBlob::getTableStructureAndFormatFromData(object_storage.get(), configuration, std::nullopt, context).first;
        return StorageAzureBlob::getTableStructureFromData(object_storage.get(), configuration, std::nullopt, context);
    }

    return parseColumnsListFromString(configuration.structure, context);
}

bool TableFunctionAzureBlobStorage::supportsReadingSubsetOfColumns(const ContextPtr & context)
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration.format, context);
}

std::unordered_set<String> TableFunctionAzureBlobStorage::getVirtualsToCheckBeforeUsingStructureHint() const
{
    return VirtualColumnUtils::getVirtualNamesForFileLikeStorage();
}

StoragePtr TableFunctionAzureBlobStorage::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto client = StorageAzureBlob::createClient(configuration, !is_insert_query);
    auto settings = StorageAzureBlob::createSettings(context);

    ColumnsDescription columns;
    if (configuration.structure != "auto")
        columns = parseColumnsListFromString(configuration.structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    StoragePtr storage = std::make_shared<StorageAzureBlob>(
        configuration,
        std::make_unique<AzureObjectStorage>(table_name, std::move(client), std::move(settings), configuration.container),
        context,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        String{},
        /// No format_settings for table function Azure
        std::nullopt,
        /* distributed_processing */ false,
        nullptr);

    storage->startup();

    return storage;
}

void registerTableFunctionAzureBlobStorage(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionAzureBlobStorage>(
        {.documentation
         = {.description=R"(The table function can be used to read the data stored on Azure Blob Storage.)",
            .examples{{"azureBlobStorage", "SELECT * FROM  azureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])", ""}}},
         .allow_readonly = false});
}

}

#endif
