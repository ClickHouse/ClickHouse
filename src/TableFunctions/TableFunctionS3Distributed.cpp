#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#include <Storages/StorageS3Distributed.h>

#include <DataTypes/DataTypeString.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <IO/S3Common.h>
#include <Storages/StorageS3.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/ClientInfo.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/TableFunctionS3Distributed.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Sources/SourceFromInputStream.h>

#include "registerTableFunctions.h"

#include <memory>
#include <thread>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


void TableFunctionS3Distributed::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    const auto message = fmt::format(
        "The signature of table function {} could be the following:\n" \
        " - cluster, url, format, structure\n" \
        " - cluster, url, format, structure, compression_method\n" \
        " - cluster, url, access_key_id, secret_access_key, format, structure\n" \
        " - cluster, url, access_key_id, secret_access_key, format, structure, compression_method",
        getName());

    if (args.size() < 4 || args.size() > 7)
        throw Exception(message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    cluster_name = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    filename = args[1]->as<ASTLiteral &>().value.safeGet<String>();

    if (args.size() == 4)
    {
        format = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else if (args.size() == 5)
    {
        format = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[3]->as<ASTLiteral &>().value.safeGet<String>();
        compression_method = args[4]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else if (args.size() == 6)
    {
        access_key_id = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        secret_access_key = args[3]->as<ASTLiteral &>().value.safeGet<String>();
        format = args[4]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[5]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else if (args.size() == 7)
    {
        access_key_id = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        secret_access_key = args[3]->as<ASTLiteral &>().value.safeGet<String>();
        format = args[4]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[5]->as<ASTLiteral &>().value.safeGet<String>();
        compression_method = args[4]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else
        throw Exception(message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}


ColumnsDescription TableFunctionS3Distributed::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionS3Distributed::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    StoragePtr storage = StorageS3Distributed::create(
        filename, access_key_id, secret_access_key, StorageID(getDatabaseName(), table_name),
        cluster_name, format, context->getSettingsRef().s3_max_connections,
        getActualTableStructure(context), ConstraintsDescription{},
        context, compression_method);

    storage->startup();

    return storage;
}


void registerTableFunctionS3Distributed(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3Distributed>();
}

void registerTableFunctionCOSDistributed(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCOSDistributed>();
}


}

#endif
