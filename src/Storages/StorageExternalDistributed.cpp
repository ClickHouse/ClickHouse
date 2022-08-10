#include "StorageExternalDistributed.h"


#include <Storages/StorageFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/ASTLiteral.h>
#include <Common/parseAddress.h>
#include <QueryPipeline/Pipe.h>
#include <Common/parseRemoteDescription.h>
#include <Storages/StorageMySQL.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/StoragePostgreSQL.h>
#include <Storages/StorageURL.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <base/logger_useful.h>


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
    const ExternalDataSourceConfiguration & configuration,
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
                    configuration.database,
                    addresses,
                    configuration.username,
                    configuration.password);

                shard = StorageMySQL::create(
                    table_id_,
                    std::move(pool),
                    configuration.database,
                    configuration.table,
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
                StoragePostgreSQLConfiguration postgres_conf;
                postgres_conf.set(configuration);
                postgres_conf.addresses = addresses;

                auto pool = std::make_shared<postgres::PoolWithFailover>(
                    postgres_conf,
                    context->getSettingsRef().postgresql_connection_pool_size,
                    context->getSettingsRef().postgresql_connection_pool_wait_timeout);

                shard = StoragePostgreSQL::create(table_id_, std::move(pool), configuration.table, columns_, constraints_, String{});
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
    (void)configuration;
    (void)cluster_description;
    (void)addresses;
    (void)table_engine;
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
            shard = std::make_shared<StorageURL>(
                url_description, table_id, format_name, format_settings, columns, constraints, String{}, context, compression_method);

            LOG_DEBUG(&Poco::Logger::get("StorageURLDistributed"), "Adding URL: {}", url_description);
        }

        shards.emplace(std::move(shard));
    }
}


Pipe StorageExternalDistributed::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
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
            storage_snapshot,
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
        if (engine_args.size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine ExternalDistributed must have at least 2 arguments: engine_name, named_collection and/or description");

        auto engine_name = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        StorageExternalDistributed::ExternalStorageEngine table_engine;
        if (engine_name == "URL")
            table_engine = StorageExternalDistributed::ExternalStorageEngine::URL;
        else if (engine_name == "MySQL")
            table_engine = StorageExternalDistributed::ExternalStorageEngine::MySQL;
        else if (engine_name == "PostgreSQL")
            table_engine = StorageExternalDistributed::ExternalStorageEngine::PostgreSQL;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "External storage engine {} is not supported for StorageExternalDistributed. Supported engines are: MySQL, PostgreSQL, URL",
                engine_name);

        ASTs inner_engine_args(engine_args.begin() + 1, engine_args.end());
        String cluster_description;

        if (engine_name == "URL")
        {
            URLBasedDataSourceConfiguration configuration;
            if (auto named_collection = getURLBasedDataSourceConfiguration(inner_engine_args, args.getLocalContext()))
            {
                auto [common_configuration, storage_specific_args] = named_collection.value();
                configuration.set(common_configuration);

                for (const auto & [name, value] : storage_specific_args)
                {
                    if (name == "description")
                        cluster_description = value->as<ASTLiteral>()->value.safeGet<String>();
                    else
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                        "Unknown key-value argument {} for table engine URL", name);
                }

                if (cluster_description.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Engine ExternalDistribued must have `description` key-value argument or named collection parameter");
            }
            else
            {
                for (auto & engine_arg : engine_args)
                    engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

                cluster_description = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
                configuration.format = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
                configuration.compression_method = "auto";
                if (engine_args.size() == 4)
                    configuration.compression_method = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
            }


            auto format_settings = StorageURL::getFormatSettingsFromArgs(args);

            return StorageExternalDistributed::create(
                cluster_description,
                args.table_id,
                configuration.format,
                format_settings,
                configuration.compression_method,
                args.columns,
                args.constraints,
                args.getContext());
        }
        else
        {
            ExternalDataSourceConfiguration configuration;
            if (auto named_collection = getExternalDataSourceConfiguration(inner_engine_args, args.getLocalContext()))
            {
                auto [common_configuration, storage_specific_args, _] = named_collection.value();
                configuration.set(common_configuration);

                for (const auto & [name, value] : storage_specific_args)
                {
                    if (name == "description")
                        cluster_description = value->as<ASTLiteral>()->value.safeGet<String>();
                    else
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                        "Unknown key-value argument {} for table function URL", name);
                }

                if (cluster_description.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Engine ExternalDistribued must have `description` key-value argument or named collection parameter");
            }
            else
            {
                if (engine_args.size() != 6)
                    throw Exception(
                        "Storage ExternalDistributed requires 5 parameters: "
                        "ExternalDistributed('engine_name', 'cluster_description', 'database', 'table', 'user', 'password').",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

                cluster_description = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
                configuration.database = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
                configuration.table = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
                configuration.username = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();
                configuration.password = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();
            }


            return StorageExternalDistributed::create(
                args.table_id,
                table_engine,
                cluster_description,
                configuration,
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
