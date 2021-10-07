#include "StorageExternalDistributed.h"


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
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/StoragePostgreSQL.h>
#include <Storages/StorageURL.h>
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
    const String & comment,
    ContextPtr context)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    size_t max_addresses = context->getSettingsRef().glob_expansion_max_elements;
    std::vector<String> shards_descriptions = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', max_addresses);
    std::vector<std::pair<std::string, UInt16>> addresses;

#if USE_MYSQL || USE_LIBPQXX

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
                    columns_,
                    constraints_,
                    String{},
                    context,
                    MySQLSettings{});
                break;
            }
#endif
#if USE_LIBPQXX

            case ExternalStorageEngine::PostgreSQL:
            {
                addresses = parseRemoteDescriptionForExternalDatabase(shard_description, max_addresses, 5432);

                auto pool = std::make_shared<postgres::PoolWithFailover>(
                    remote_database,
                    addresses,
                    username, password,
                    context->getSettingsRef().postgresql_connection_pool_size,
                    context->getSettingsRef().postgresql_connection_pool_wait_timeout);

                shard = StoragePostgreSQL::create(table_id_, std::move(pool), remote_table, columns_, constraints_, String{});
                break;
            }
#endif
            default:
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Unsupported table engine. Supported engines are: MySQL, PostgreSQL, URL");
            }
        }

        shards.emplace(std::move(shard));
    }

#else
    (void)table_engine;
    (void)remote_database;
    (void)remote_table;
    (void)username;
    (void)password;
    (void)shards_descriptions;
    (void)addresses;
#endif
}


StorageExternalDistributed::StorageExternalDistributed(
            const String & addresses_description,
            const StorageID & table_id,
            const String & format_name,
            const std::optional<FormatSettings> & format_settings,
            const String & compression_method,
            const ColumnsDescription & columns,
            const ConstraintsDescription & constraints,
            ContextPtr context)
        : IStorage(table_id)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setConstraints(constraints);
    setInMemoryMetadata(storage_metadata);

    size_t max_addresses = context->getSettingsRef().glob_expansion_max_elements;
    /// Generate addresses without splitting for failover options
    std::vector<String> url_descriptions = parseRemoteDescription(addresses_description, 0, addresses_description.size(), ',', max_addresses);
    std::vector<String> uri_options;

    for (const auto & url_description : url_descriptions)
    {
        /// For each uri (which acts like shard) check if it has failover options
        uri_options = parseRemoteDescription(url_description, 0, url_description.size(), '|', max_addresses);
        StoragePtr shard;

        if (uri_options.size() > 1)
        {
            shard = std::make_shared<StorageURLWithFailover>(
                uri_options,
                table_id,
                format_name,
                format_settings,
                columns, constraints, context,
                compression_method);
        }
        else
        {
            Poco::URI uri(url_description);
            shard = std::make_shared<StorageURL>(
                uri, table_id, format_name, format_settings, columns, constraints, String{}, context, compression_method);

            LOG_DEBUG(&Poco::Logger::get("StorageURLDistributed"), "Adding URL: {}", url_description);
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
        const String & addresses_description = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        StorageExternalDistributed::ExternalStorageEngine table_engine;
        if (engine_name == "URL")
        {
            table_engine = StorageExternalDistributed::ExternalStorageEngine::URL;

            const String & format_name = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
            String compression_method = "auto";
            if (engine_args.size() == 4)
                compression_method = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();

            auto format_settings = StorageURL::getFormatSettingsFromArgs(args);

            return StorageExternalDistributed::create(
                addresses_description,
                args.table_id,
                format_name,
                format_settings,
                compression_method,
                args.columns,
                args.constraints,
                args.getContext());
        }
        else
        {
            if (engine_name == "MySQL")
                table_engine = StorageExternalDistributed::ExternalStorageEngine::MySQL;
            else if (engine_name == "PostgreSQL")
                table_engine = StorageExternalDistributed::ExternalStorageEngine::PostgreSQL;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "External storage engine {} is not supported for StorageExternalDistributed. Supported engines are: MySQL, PostgreSQL, URL",
                    engine_name);

            const String & remote_database = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
            const String & remote_table = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
            const String & username = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();
            const String & password = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();

            return StorageExternalDistributed::create(
                args.table_id,
                table_engine,
                addresses_description,
                remote_database,
                remote_table,
                username,
                password,
                args.columns,
                args.constraints,
                args.comment,
                args.getContext());
        }
    },
    {
        .source_access_type = AccessType::SOURCES,
    });
}

}
