#include <Common/config.h>

#if USE_HDFS

#include <Storages/HDFS/StorageHDFSCluster.h>

#include <DataTypes/DataTypeString.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/HDFS/StorageHDFS.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/ClientInfo.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionHDFS.h>
#include <TableFunctions/TableFunctionHDFSCluster.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST_fwd.h>

#include "registerTableFunctions.h"

#include <memory>
#include <thread>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


void TableFunctionHDFSCluster::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    const auto message = fmt::format(
        "The signature of table function {} shall be the following:\n" \
        " - cluster, uri, format, structure",
        " - cluster, uri, format, structure, compression_method",
        getName());

    if (args.size() < 4 || args.size() > 5)
        throw Exception(message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    /// This arguments are always the first
    cluster_name = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    uri = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    format = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    structure = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    if (args.size() >= 5)
        compression_method = args[4]->as<ASTLiteral &>().value.safeGet<String>();
}


ColumnsDescription TableFunctionHDFSCluster::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionHDFSCluster::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    StoragePtr storage;
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// On worker node this uri won't contains globs
        storage = StorageHDFS::create(
            uri,
            StorageID(getDatabaseName(), table_name),
            format,
            getActualTableStructure(context),
            ConstraintsDescription{},
            String{},
            context,
            compression_method,
            /*distributed_processing=*/true,
            nullptr);
    }
    else
    {
        storage = StorageHDFSCluster::create(
            cluster_name, uri, StorageID(getDatabaseName(), table_name),
            format, getActualTableStructure(context), ConstraintsDescription{},
            compression_method);
    }

    storage->startup();

    return storage;
}


void registerTableFunctionHDFSCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHDFSCluster>();
}


}

#endif
