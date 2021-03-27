#include "StorageMySQLDistributed.h"

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


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StorageMySQLDistributed::StorageMySQLDistributed(
    const StorageID & table_id_,
    const String & remote_database_,
    const String & remote_table_,
    const String & cluster_description,
    const String & username,
    const String & password,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : IStorage(table_id_)
    , remote_database(remote_database_)
    , remote_table(remote_table_)
    , context(context_.getGlobalContext())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    size_t max_addresses = context.getSettingsRef().table_function_remote_max_addresses;

    /// Split into shards
    std::vector<String> shards_descriptions = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', max_addresses);

    /// For each shard make a connection pool of its replicas, replicas a parsed in pool
    for (const auto & shard_description : shards_descriptions)
    {
        /// Parse shard description like host-{01..02}-{1|2|3}:port, into host_description (host-01-{1..2}-{1|2|3}) and port
        auto parsed_host_port = parseAddress(shard_description, 3306);
        shards.emplace(std::make_shared<mysqlxx::PoolWithFailover>(remote_database, parsed_host_port.first, parsed_host_port.second, username, password));
    }
}


Pipe StorageMySQLDistributed::read(
    const Names & column_names_,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info_,
    const Context & context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size_,
    unsigned)
{
    metadata_snapshot->check(column_names_, getVirtuals(), getStorageID());
    String query = transformQueryForExternalDatabase(
        query_info_,
        metadata_snapshot->getColumns().getOrdinary(),
        IdentifierQuotingStyle::BackticksMySQL,
        remote_database,
        remote_table,
        context_);

    Block sample_block;
    for (const String & column_name : column_names_)
    {
        auto column_data = metadata_snapshot->getColumns().getPhysical(column_name);

        WhichDataType which(column_data.type);
        /// Convert enum to string.
        if (which.isEnum())
            column_data.type = std::make_shared<DataTypeString>();
        sample_block.insert({ column_data.type, column_data.name });
    }

    Pipes pipes;
    for (const auto & shard : shards)
    {
        pipes.emplace_back(std::make_shared<SourceFromInputStream>(
            std::make_shared<MySQLWithFailoverBlockInputStream>(shard, query, sample_block, max_block_size_, /* auto_close = */ true)));
    }

    return Pipe::unitePipes(std::move(pipes));
}


void registerStorageMySQLDistributed(StorageFactory & factory)
{
    factory.registerStorage("MySQLDistributed", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 5)
            throw Exception(
                "Storage MySQLiDistributed requires 5 parameters: MySQLDistributed('cluster_description', database, table, 'user', 'password').",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.local_context);

        const String & remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & cluster_description = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        const String & username = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();
        const String & password = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();

        return StorageMySQLDistributed::create(
            args.table_id,
            remote_database,
            remote_table,
            cluster_description,
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
