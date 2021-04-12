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

    filename = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    if (args.size() == 3)
    {
        format = args[1]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else if (args.size() == 4)
    {
        format = args[1]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        compression_method = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else if (args.size() == 5)
    {
        access_key_id = args[1]->as<ASTLiteral &>().value.safeGet<String>();
        secret_access_key = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        format = args[3]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[4]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else if (args.size() == 6)
    {
        access_key_id = args[1]->as<ASTLiteral &>().value.safeGet<String>();
        secret_access_key = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        format = args[3]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[4]->as<ASTLiteral &>().value.safeGet<String>();
        compression_method = args[5]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else
    {
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
}

ColumnsDescription TableFunctionS3::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionS3::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    Poco::URI uri (filename);
    S3::URI s3_uri (uri);
    UInt64 min_upload_part_size = context->getSettingsRef().s3_min_upload_part_size;
    UInt64 max_single_part_upload_size = context->getSettingsRef().s3_max_single_part_upload_size;
    UInt64 max_connections = context->getSettingsRef().s3_max_connections;

    StoragePtr storage = StorageS3::create(
            s3_uri,
            access_key_id,
            secret_access_key,
            StorageID(getDatabaseName(), table_name),
            format,
            min_upload_part_size,
            max_single_part_upload_size,
            max_connections,
            getActualTableStructure(context),
            ConstraintsDescription{},
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
