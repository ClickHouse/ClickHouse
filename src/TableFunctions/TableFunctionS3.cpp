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
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StoragePtr TableFunctionS3::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.size() < 3 || args.size() > 6)
        throw Exception("Table function '" + getName() + "' requires 3 to 6 arguments: url, [access_key_id, secret_access_key,] format, structure and [compression_method].",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    String filename = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    String format;
    String structure;
    String access_key_id;
    String secret_access_key;

    if (args.size() < 5)
    {
        format = args[1]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else
    {
        access_key_id = args[1]->as<ASTLiteral &>().value.safeGet<String>();
        secret_access_key = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        format = args[3]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[4]->as<ASTLiteral &>().value.safeGet<String>();
    }

    String compression_method;
    if (args.size() == 4 || args.size() == 6)
        compression_method = args.back()->as<ASTLiteral &>().value.safeGet<String>();
    else
        compression_method = "auto";

    ColumnsDescription columns = parseColumnsListFromString(structure, context);

    static constexpr auto S3 = "S3", s3 = "s3", COSN = "COSN", cons = "cosn";
    Poco::URI uri (filename);
    S3::URI s3_uri (uri);
    if (s3_uri.storage_name == S3) {
        name = s3;
        storage_type_name = S3;
    } else {
        name = cosn;
        storage_type_name = COSN;
    }
    /// Create table
    StoragePtr storage = getStorage(s3_uri, access_key_id, secret_access_key, format, columns, const_cast<Context &>(context), table_name, compression_method);

    storage->startup();

    return storage;
}

StoragePtr TableFunctionS3::getStorage(
    const URI & uri,
    const String & access_key_id,
    const String & secret_access_key,
    const String & format,
    const ColumnsDescription & columns,
    Context & global_context,
    const std::string & table_name,
    const String & compression_method)
{

    UInt64 min_upload_part_size = global_context.getSettingsRef().s3_min_upload_part_size;
    return StorageS3::create(
        s3_uri,
        access_key_id,
        secret_access_key,
        StorageID(getDatabaseName(), table_name),
        format,
        min_upload_part_size,
        columns,
        ConstraintsDescription{},
        global_context,
        compression_method);
}

void registerTableFunctionS3(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>();
}

void registerTableFunctionCOS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>();
}

}

#endif
