#include <Common/config.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Storages/StorageS3.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTLiteral.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionS3::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    const auto message = fmt::format(
        "The signature of table function {} could be the following:\n" \
        " - url, format, structure\n" \
        " - url, format, structure, compression_method\n" \
        " - url, access_key_id, secret_access_key, format, structure\n" \
        " - url, access_key_id, secret_access_key, format, structure, compression_method",
        getName());

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    if (args.size() < 3 || args.size() > 6)
        throw Exception(message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    /// Size -> argument indexes
    static auto size_to_args = std::map<size_t, std::map<String, size_t>>
    {
        {3, {{"format", 1}, {"structure", 2}}},
        {4, {{"format", 1}, {"structure", 2}, {"compression_method", 3}}},
        {5, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}}},
        {6, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}, {"compression_method", 5}}}
    };

    /// This argument is always the first
    filename = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    auto & args_to_idx = size_to_args[args.size()];

    if (args_to_idx.contains("format"))
        format = args[args_to_idx["format"]]->as<ASTLiteral &>().value.safeGet<String>();

    if (args_to_idx.contains("structure"))
        structure = args[args_to_idx["structure"]]->as<ASTLiteral &>().value.safeGet<String>();

    if (args_to_idx.contains("compression_method"))
        compression_method = args[args_to_idx["compression_method"]]->as<ASTLiteral &>().value.safeGet<String>();

    if (args_to_idx.contains("access_key_id"))
        access_key_id = args[args_to_idx["access_key_id"]]->as<ASTLiteral &>().value.safeGet<String>();

    if (args_to_idx.contains("secret_access_key"))
        secret_access_key = args[args_to_idx["secret_access_key"]]->as<ASTLiteral &>().value.safeGet<String>();
}

ColumnsDescription TableFunctionS3::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionS3::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    Poco::URI uri (filename);
    S3::URI s3_uri (uri);
    UInt64 max_single_read_retries = context->getSettingsRef().s3_max_single_read_retries;
    UInt64 min_upload_part_size = context->getSettingsRef().s3_min_upload_part_size;
    UInt64 max_single_part_upload_size = context->getSettingsRef().s3_max_single_part_upload_size;
    UInt64 max_connections = context->getSettingsRef().s3_max_connections;

    StoragePtr storage = StorageS3::create(
        s3_uri,
        access_key_id,
        secret_access_key,
        StorageID(getDatabaseName(), table_name),
        format,
        max_single_read_retries,
        min_upload_part_size,
        max_single_part_upload_size,
        max_connections,
        getActualTableStructure(context),
        ConstraintsDescription{},
        String{},
        context,
        compression_method);

    storage->startup();

    return storage;
}


void registerTableFunctionS3(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>();
}

void registerTableFunctionCOS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCOS>();
}

}

#endif
