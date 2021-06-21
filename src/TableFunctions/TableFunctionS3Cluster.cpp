#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#include <Storages/StorageS3Cluster.h>

#include <DataTypes/DataTypeString.h>
#include <DataStreams/RemoteBlockInputStream.h>
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


void TableFunctionS3Cluster::parseArguments(const ASTPtr & ast_function, ContextPtr context)
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

    /// This arguments are always the first
    cluster_name = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    filename = args[1]->as<ASTLiteral &>().value.safeGet<String>();

    /// Size -> argument indexes
    static auto size_to_args = std::map<size_t, std::map<String, size_t>>
    {
        {4, {{"format", 2}, {"structure", 3}}},
        {5, {{"format", 2}, {"structure", 3}, {"compression_method", 4}}},
        {6, {{"access_key_id", 2}, {"secret_access_key", 3}, {"format", 4}, {"structure", 5}}},
        {7, {{"access_key_id", 2}, {"secret_access_key", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}}}
    };

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


ColumnsDescription TableFunctionS3Cluster::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionS3Cluster::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    StoragePtr storage;
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// On worker node this filename won't contains globs
        Poco::URI uri (filename);
        S3::URI s3_uri (uri);
        /// Actually this parameters are not used
        UInt64 max_single_read_retries = context->getSettingsRef().s3_max_single_read_retries;
        UInt64 min_upload_part_size = context->getSettingsRef().s3_min_upload_part_size;
        UInt64 max_single_part_upload_size = context->getSettingsRef().s3_max_single_part_upload_size;
        UInt64 max_connections = context->getSettingsRef().s3_max_connections;
        storage = StorageS3::create(
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
            compression_method,
            /*distributed_processing=*/true);
    }
    else
    {
        storage = StorageS3Cluster::create(
            filename, access_key_id, secret_access_key, StorageID(getDatabaseName(), table_name),
            cluster_name, format, context->getSettingsRef().s3_max_connections,
            getActualTableStructure(context), ConstraintsDescription{},
            context, compression_method);
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
