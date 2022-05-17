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
    extern const int BAD_GET;
}


void TableFunctionHDFSCluster::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    auto ast_copy = ast_function->clone();
    /// Parse args
    ASTs & args_func = ast_copy->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    const auto message = fmt::format(
        "The signature of table function {} shall be the following:\n" \
        " - cluster, uri\n",\
        " - cluster, format\n",\
        " - cluster, uri, format, structure\n",\
        " - cluster, uri, format, structure, compression_method",
        getName());

    if (args.size() < 2 || args.size() > 5)
        throw Exception(message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    /// This argument is always the first
    cluster_name = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    if (!context->tryGetCluster(cluster_name))
        throw Exception(ErrorCodes::BAD_GET, "Requested cluster '{}' not found", cluster_name);

     /// Just cut the first arg (cluster_name) and try to parse other table function arguments as is
    args.erase(args.begin());

    ITableFunctionFileLike::parseArguments(ast_copy, context);
}


ColumnsDescription TableFunctionHDFSCluster::getActualTableStructure(ContextPtr context) const
{
    if (structure == "auto")
        return StorageHDFS::getTableStructureFromData(format, filename, compression_method, context);

    return parseColumnsListFromString(structure, context);
}


StoragePtr TableFunctionHDFSCluster::getStorage(
    const String & /*source*/, const String & /*format_*/, const ColumnsDescription &, ContextPtr context,
    const std::string & table_name, const String & /*compression_method_*/) const
{
    StoragePtr storage;
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// On worker node this uri won't contains globs
        storage = std::make_shared<StorageHDFS>(
            filename,
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
        storage = std::make_shared<StorageHDFSCluster>(
            context,
            cluster_name, filename, StorageID(getDatabaseName(), table_name),
            format, getActualTableStructure(context), ConstraintsDescription{},
            compression_method);
    }
    return storage;
}

void registerTableFunctionHDFSCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHDFSCluster>();
}


}

#endif
