#include <Common/config.h>

#if USE_AWS_S3

#include <Storages/StorageS3Cluster.h>

#include <DataTypes/DataTypeString.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <IO/S3Common.h>
#include <Storages/StorageS3.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/ClientInfo.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/TableFunctionS3Cluster.h>
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


void TableFunctionS3Cluster::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    for (auto & arg : args)
        arg = evaluateConstantExpressionAsLiteral(arg, context);

    const auto message = fmt::format(
        "The signature of table function {} could be the following:\n" \
        " - cluster, url\n"
        " - cluster, url, format\n" \
        " - cluster, url, format, structure\n" \
        " - cluster, url, access_key_id, secret_access_key\n" \
        " - cluster, url, format, structure, compression_method\n" \
        " - cluster, url, access_key_id, secret_access_key, format\n"
        " - cluster, url, access_key_id, secret_access_key, format, structure\n" \
        " - cluster, url, access_key_id, secret_access_key, format, structure, compression_method",
        getName());

    if (args.size() < 2 || args.size() > 7)
        throw Exception(message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// This arguments are always the first
    configuration.cluster_name = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    if (!context->tryGetCluster(configuration.cluster_name))
        throw Exception(ErrorCodes::BAD_GET, "Requested cluster '{}' not found", configuration.cluster_name);

    /// Just cut the first arg (cluster_name) and try to parse s3 table function arguments as is
    ASTs clipped_args;
    clipped_args.reserve(args.size());
    std::copy(args.begin() + 1, args.end(), std::back_inserter(clipped_args));

    /// StorageS3ClusterConfiguration inherints from StorageS3Configuration, so it is safe to upcast it.
    TableFunctionS3::parseArgumentsImpl(message, clipped_args, context, static_cast<StorageS3Configuration & >(configuration));
}


ColumnsDescription TableFunctionS3Cluster::getActualTableStructure(ContextPtr context) const
{
    if (configuration.structure == "auto")
    {
        return StorageS3::getTableStructureFromData(
            configuration.format,
            S3::URI(Poco::URI(configuration.url)),
            configuration.auth_settings.access_key_id,
            configuration.auth_settings.secret_access_key,
            configuration.compression_method,
            false,
            std::nullopt,
            context);
    }

    return parseColumnsListFromString(configuration.structure, context);
}

StoragePtr TableFunctionS3Cluster::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    StoragePtr storage;

    ColumnsDescription columns;
    if (configuration.structure != "auto")
        columns = parseColumnsListFromString(configuration.structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// On worker node this filename won't contains globs
        Poco::URI uri (configuration.url);
        S3::URI s3_uri (uri);
        storage = StorageS3::create(
            s3_uri,
            configuration.auth_settings.access_key_id,
            configuration.auth_settings.secret_access_key,
            StorageID(getDatabaseName(), table_name),
            configuration.format,
            configuration.rw_settings,
            columns,
            ConstraintsDescription{},
            String{},
            context,
            // No format_settings for S3Cluster
            std::nullopt,
            configuration.compression_method,
            /*distributed_processing=*/true);
    }
    else
    {
        storage = StorageS3Cluster::create(
            configuration.url,
            configuration.auth_settings.access_key_id,
            configuration.auth_settings.secret_access_key,
            StorageID(getDatabaseName(), table_name),
            configuration.cluster_name, configuration.format,
            columns,
            ConstraintsDescription{},
            context,
            configuration.compression_method);
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
