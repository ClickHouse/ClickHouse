#include "config.h"

#if USE_AZURE_BLOB_STORAGE

//#include <IO/S3Common.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/TableFunctionAzure.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageAzure.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageURL.h>
#include <Storages/NamedCollectionsHelpers.h>
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
    extern const int LOGICAL_ERROR;
}


void TableFunctionAzure::parseArgumentsImpl(ASTs & args, const ContextPtr & context)
{
    if (args.size() != 5)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The signature of table function {} shall be the following:\n{}", getName(), getSignature());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    configuration.connection_url = checkAndGetLiteralArgument<String>(args[0], "connection_url");
    configuration.container = checkAndGetLiteralArgument<String>(args[1], "container");
    configuration.blob_path = checkAndGetLiteralArgument<String>(args[2], "blob_path");
    configuration.format = checkAndGetLiteralArgument<String>(args[3], "format");
    configuration.structure = checkAndGetLiteralArgument<String>(args[4], "structure");
}

void TableFunctionAzure::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    LOG_INFO(&Poco::Logger::get("TableFunctionAzure"), "parseArguments = {}", ast_function->dumpTree());

    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    auto & args = args_func.at(0)->children;

    parseArgumentsImpl(args, context);
}

ColumnsDescription TableFunctionAzure::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(configuration.structure, context);
}

bool TableFunctionAzure::supportsReadingSubsetOfColumns()
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration.format);
}

StoragePtr TableFunctionAzure::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    LOG_INFO(&Poco::Logger::get("TableFunctionAzure"), "executeImpl  = {}", table_name);

    ColumnsDescription columns;
    columns = parseColumnsListFromString(configuration.structure, context);

    configuration.is_connection_string = true;
    configuration.blobs_paths = {configuration.blob_path};

    auto client = StorageAzure::createClient(configuration);

    StoragePtr storage = std::make_shared<StorageAzure>(
        configuration,
        std::make_unique<AzureObjectStorage>(table_name, std::move(client), std::make_unique<AzureObjectStorageSettings>()),
        context,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        String{},
        /// No format_settings for table function Azure
        std::nullopt, nullptr);

    storage->startup();

    return storage;
}

void registerTableFunctionAzure(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionAzure>(
        {.documentation
         = {.description=R"(The table function can be used to read the data stored on Azure Blob Storage.)",
            .examples{{"azure_blob", "SELECT * FROM  azure_blob(connection, container, blob_path, format, structure)", ""}}},
         .allow_readonly = false});
}

}

#endif
