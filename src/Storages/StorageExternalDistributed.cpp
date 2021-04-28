#include "StorageExternalDistributed.h"

#if USE_MYSQL || USE_LIBPQXX

#include <Storages/StorageFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTLiteral.h>
#include <Common/parseAddress.h>
#include <Processors/Pipe.h>
#include <Common/parseRemoteDescription.h>
#include <Storages/StorageMySQL.h>
#include <Storages/StoragePostgreSQL.h>
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
    ExternalStorageEngine table_engine,
    const String & cluster_description,
    const String & remote_database,
    const String & remote_table,
    const String & username,
    const String & password,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    size_t max_addresses = context->getSettingsRef().glob_expansion_max_elements;
    std::vector<String> shards_descriptions = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', max_addresses);
    std::vector<std::pair<std::string, UInt16>> addresses;

    /// For each shard pass replicas description into storage, replicas are managed by storage's PoolWithFailover.
    for (const auto & shard_description : shards_descriptions)
    {
        StoragePtr shard;

        switch (table_engine)
        {
#if USE_MYSQL
            case ExternalStorageEngine::MySQL:
            {
                addresses = parseRemoteDescriptionForExternalDatabase(shard_description, max_addresses, 3306);

                mysqlxx::PoolWithFailover pool(
                    remote_database,
                    addresses,
                    username, password);

                shard = StorageMySQL::create(
                    table_id_,
                    std::move(pool),
                    remote_database,
                    remote_table,
                    /* replace_query = */ false,
                    /* on_duplicate_clause = */ "",
                    columns_, constraints_,
                    context);
                break;
            }
#endif
#if USE_LIBPQXX

            case ExternalStorageEngine::PostgreSQL:
            {
                addresses = parseRemoteDescriptionForExternalDatabase(shard_description, max_addresses, 5432);

                postgres::PoolWithFailover pool(
                    remote_database,
                    addresses,
                    username, password,
                    context->getSettingsRef().postgresql_connection_pool_size,
                    context->getSettingsRef().postgresql_connection_pool_wait_timeout);

                shard = StoragePostgreSQL::create(
                    table_id_,
                    std::move(pool),
                    remote_table,
                    columns_, constraints_,
                    context);
                break;
            }
#endif
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Unsupported table engine. Supported engines are: MySQL, PostgreSQL");
        }

        shards.emplace(std::move(shard));
    }
}


Pipe StorageExternalDistributed::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
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
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

        const String & engine_name = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        const String & cluster_description = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_database = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_table = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        const String & username = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();
        const String & password = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();

        StorageExternalDistributed::ExternalStorageEngine table_engine;
        if (engine_name == "MySQL")
            table_engine = StorageExternalDistributed::ExternalStorageEngine::MySQL;
        else if (engine_name == "PostgreSQL")
            table_engine = StorageExternalDistributed::ExternalStorageEngine::PostgreSQL;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "External storage engine {} is not supported for StorageExternalDistributed. Supported engines are: MySQL, PostgreSQL",
                engine_name);

        return StorageExternalDistributed::create(
            args.table_id,
            table_engine,
            cluster_description,
            remote_database,
            remote_table,
            username,
            password,
            args.columns,
            args.constraints,
            args.getContext());
    },
    {
        .source_access_type = AccessType::MYSQL,
    });
}

}

#endif
