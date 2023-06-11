#include "config.h"

#if USE_AWS_S3

#include <Storages/StorageS3.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <DataTypes/DataTypeString.h>
#include <IO/S3Common.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/ClientInfo.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/TableFunctionS3Cluster.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
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


void TableFunctionS3Cluster::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    constexpr auto fmt_string = "The signature of table function {} could be the following:\n"
                                " - cluster, url\n"
                                " - cluster, url, format\n"
                                " - cluster, url, format, structure\n"
                                " - cluster, url, access_key_id, secret_access_key\n"
                                " - cluster, url, format, structure, compression_method\n"
                                " - cluster, url, access_key_id, secret_access_key, format\n"
                                " - cluster, url, access_key_id, secret_access_key, format, structure\n"
                                " - cluster, url, access_key_id, secret_access_key, format, structure, compression_method";
    auto message = PreformattedMessage{fmt::format(fmt_string, getName()), fmt_string};
    if (args.size() < 2 || args.size() > 7)
        throw Exception::createDeprecated(message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// This arguments are always the first
    configuration.cluster_name = checkAndGetLiteralArgument<String>(args[0], "cluster_name");

    if (!context->tryGetCluster(configuration.cluster_name))
        throw Exception(ErrorCodes::BAD_GET, "Requested cluster '{}' not found", configuration.cluster_name);

    /// Just cut the first arg (cluster_name) and try to parse s3 table function arguments as is
    ASTs clipped_args;
    clipped_args.reserve(args.size());
    std::copy(args.begin() + 1, args.end(), std::back_inserter(clipped_args));

    /// StorageS3ClusterConfiguration inherints from StorageS3::Configuration, so it is safe to upcast it.
    TableFunctionS3::parseArgumentsImpl(message.text, clipped_args, context, static_cast<StorageS3::Configuration &>(configuration));
}


ColumnsDescription TableFunctionS3Cluster::getActualTableStructure(ContextPtr context) const
{
    context->checkAccess(getSourceAccessType());

    if (configuration.structure == "auto")
        return StorageS3::getTableStructureFromData(configuration, std::nullopt, context);

    return parseColumnsListFromString(configuration.structure, context);
}

StoragePtr TableFunctionS3Cluster::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    StoragePtr storage;
    ColumnsDescription columns;
    bool structure_argument_was_provided = configuration.structure != "auto";

    if (structure_argument_was_provided)
    {
        columns = parseColumnsListFromString(configuration.structure, context);
    }
    else if (!structure_hint.empty())
    {
        columns = structure_hint;
    }

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// On worker node this filename won't contains globs
        storage = std::make_shared<StorageS3>(
            configuration,
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            /* comment */String{},
            context,
            /* format_settings */std::nullopt, /// No format_settings for S3Cluster
            /*distributed_processing=*/true);
    }
    else
    {
        storage = std::make_shared<StorageS3Cluster>(
            configuration,
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            context,
            structure_argument_was_provided);
    }

    storage->startup();

    return storage;
}


void registerTableFunctionS3Cluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3Cluster>();
}


}

#endif
