#include <TableFunctions/TableFunctionURLCluster.h>

#include "registerTableFunctions.h"
#include <Access/Common/AccessFlags.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageExternalDistributed.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionURL.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/Context.h>
#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_GET;
}

void TableFunctionURLCluster::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    auto ast_copy = ast_function->clone();
    /// Parse args
    ASTs & args_func = ast_copy->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function {} must have arguments", getName());

    ASTs & args = args_func.at(0)->children;

    if (args.size() < 2 || args.size() > 5)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "The signature of table function {} shall be the following:\n"
            " - cluster, uri\n"
            " - cluster, uri, format\n"
            " - cluster, uri, format, structure\n"
            " - cluster, uri, format, structure, compression_method",
            getName());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    /// This argument is always the first
    cluster_name = checkAndGetLiteralArgument<String>(args[0], "cluster_name");

    if (!context->tryGetCluster(cluster_name))
        throw Exception(ErrorCodes::BAD_GET, "Requested cluster '{}' not found", cluster_name);

     /// Just cut the first arg (cluster_name) and try to parse other table function arguments as is
    args.erase(args.begin());

    TableFunctionURL::parseArguments(ast_copy, context);
}


ColumnsDescription TableFunctionURLCluster::getActualTableStructure(ContextPtr context) const
{
    if (structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        return StorageURL::getTableStructureFromData(format,
            filename,
            chooseCompressionMethod(Poco::URI(filename).getPath(), compression_method),
            configuration.headers,
            std::nullopt,
            context);
    }

    return parseColumnsListFromString(structure, context);
}


StoragePtr TableFunctionURLCluster::getStorage(
    const String & /*source*/, const String & /*format_*/, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & /*compression_method_*/) const
{
    StoragePtr storage;
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        //On worker node this uri won't contains globs
        storage = std::make_shared<StorageURL>(
            filename,
            StorageID(getDatabaseName(), table_name),
            format,
            std::nullopt /*format settings*/,
            columns,
            ConstraintsDescription{},
            String{},
            context,
            compression_method,
            configuration.headers,
            configuration.http_method,
            nullptr,
            /*distributed_processing=*/ true);
    }
    else
    {
        storage = std::make_shared<StorageURLCluster>(
            context,
            cluster_name, filename, StorageID(getDatabaseName(), table_name),
            format, getActualTableStructure(context), ConstraintsDescription{},
            compression_method, configuration, structure != "auto");
    }
    return storage;
}

void registerTableFunctionURLCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURLCluster>();
}

}
