#include "StorageExternalDistributed.h"

#if USE_MYSQL

#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Formats/MySQLBlockInputStream.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Common/parseAddress.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Common/parseAddress.h>
#include <mysqlxx/Transaction.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>
#include <Common/parseRemoteDescription.h>
#include <Storages/StorageMySQL.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StorageExternalDistributed::StorageExternalDistributed(
    const StorageID & table_id_,
    const String & engine_name,
    const String & cluster_description,
    const String & remote_database,
    const String & remote_table,
    const String & username,
    const String & password,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    /// TODO: add proper setting
    size_t max_addresses = context.getSettingsRef().table_function_remote_max_addresses;

    /// Split into shards
    std::vector<String> shards_descriptions = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', max_addresses);

    /// For each shard pass replicas description into storage, replicas are managed by storage's PoolWithFailover.
    for (const auto & shard_description : shards_descriptions)
    {
        /// Parse shard description like host-{01..02}-{1|2|3}:port, into host_description_pattern (host-01-{1..2}-{1|2|3}) and port
        /// TODO: add setting with default port
        LOG_TRACE(&Poco::Logger::get("StorageExternalDistributed"), "Adding shard description: {}", shard_description);
        auto parsed_shard_description = parseAddress(shard_description, 3306);

        StoragePtr shard;
        if (engine_name == "MySQL")
        {
            mysqlxx::PoolWithFailover pool(remote_database, parsed_shard_description.first, parsed_shard_description.second, username, password);
            shard = StorageMySQL::create(
                table_id_, std::move(pool), remote_database, remote_table,
                /* replace_query = */ false, /* on_duplicate_clause = */ "",
                columns_, constraints_, context);
        }
        else if (engine_name == "PostgreSQL")
        {
        }
        else
        {
            throw Exception(
                "External storage engine {} is not supported for StorageExternalDistributed. Supported engines are: MySQL, PostgreSQL",
                ErrorCodes::BAD_ARGUMENTS);
        }

        shards.emplace(std::move(shard));
    }
}


Pipe StorageExternalDistributed::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    Pipes pipes;
    for (const auto & shard : shards)
    {
        pipes.emplace_back(shard->read(
            column_names,
            metadata_snapshot,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams
        ));
    }

    return Pipe::unitePipes(std::move(pipes));
}


void registerStorageExternalDistributed(StorageFactory & factory)
{
    factory.registerStorage("ExternalDistributed", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 6)
            throw Exception(
                "Storage MySQLiDistributed requires 5 parameters: ExternalDistributed('engine_name', 'cluster_description', database, table, 'user', 'password').",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.local_context);

        const String & engine_name = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        const String & cluster_description = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_database = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_table = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        const String & username = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();
        const String & password = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();

        return StorageExternalDistributed::create(
            args.table_id,
            engine_name,
            cluster_description,
            remote_database,
            remote_table,
            username,
            password,
            args.columns,
            args.constraints,
            args.context);
    },
    {
        .source_access_type = AccessType::MYSQL,
    });
}

}

#endif
