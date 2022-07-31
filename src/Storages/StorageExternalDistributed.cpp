#include "StorageExternalDistributed.h"


#include <Storages/StorageFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Common/parseAddress.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/parseRemoteDescription.h>
#include <Storages/StorageMySQL.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/StoragePostgreSQL.h>
#include <Storages/StorageURL.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/logger_useful.h>
#include <Processors/QueryPlan/UnionStep.h>


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

                shard = std::make_shared<StorageMySQL>(
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

                const auto & settings = context->getSettingsRef();
                auto pool = std::make_shared<postgres::PoolWithFailover>(
                    postgres_conf,
                    settings.postgresql_connection_pool_size,
                    settings.postgresql_connection_pool_wait_timeout,
                    POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES,
                    settings.postgresql_connection_pool_auto_close_connection);

                shard = std::make_shared<StoragePostgreSQL>(table_id_, std::move(pool), configuration.table, columns_, constraints_, String{});
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


void StorageExternalDistributed::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    std::vector<std::unique_ptr<QueryPlan>> plans;
    for (const auto & shard : shards)
    {
        plans.emplace_back(std::make_unique<QueryPlan>());
        shard->read(
            *plans.back(),
            column_names,
            storage_snapshot,
            query_info,
            context,
            processed_stage,
            max_block_size,
            num_streams
        );
    }

    if (plans.empty())
    {
        auto header = storage_snapshot->getSampleBlockForColumns(column_names);
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info, context);
    }

    if (plans.size() == 1)
    {
        query_plan = std::move(*plans.front());
        return;
    }

    DataStreams input_streams;
    input_streams.reserve(plans.size());
    for (auto & plan : plans)
        input_streams.emplace_back(plan->getCurrentDataStream());

    auto union_step = std::make_unique<UnionStep>(std::move(input_streams));
    query_plan.unitePlans(std::move(union_step), std::move(plans));
}


void registerStorageExternalDistributed(StorageFactory & factory)
{
    factory.registerStorage("ExternalDistributed", [](const StorageFactory::Arguments & args)
    {
        ASTList & engine_args = args.engine_args;
        if (engine_args.size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine ExternalDistributed must have at least 2 arguments: engine_name, named_collection and/or description");

        auto engine_name = checkAndGetLiteralArgument<String>(engine_args.front(), "engine_name");
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

        ASTList inner_engine_args(++engine_args.begin(), engine_args.end());
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
                        cluster_description = checkAndGetLiteralArgument<String>(value, "cluster_description");
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

                auto it = ++engine_args.begin();
                cluster_description = checkAndGetLiteralArgument<String>(*(it++), "cluster_description");
                configuration.format = checkAndGetLiteralArgument<String>(*(it++), "format");
                configuration.compression_method = "auto";
                if (engine_args.size() == 4)
                    configuration.compression_method = checkAndGetLiteralArgument<String>(*(it++), "compression_method");
            }


            auto format_settings = StorageURL::getFormatSettingsFromArgs(args);

            return std::make_shared<StorageExternalDistributed>(
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
                        cluster_description = checkAndGetLiteralArgument<String>(value, "cluster_description");
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

                auto it = ++engine_args.begin();
                cluster_description = checkAndGetLiteralArgument<String>(*(it++), "cluster_description");
                configuration.database = checkAndGetLiteralArgument<String>(*(it++), "database");
                configuration.table = checkAndGetLiteralArgument<String>(*(it++), "table");
                configuration.username = checkAndGetLiteralArgument<String>(*(it++), "username");
                configuration.password = checkAndGetLiteralArgument<String>(*(it++), "password");
            }


            return std::make_shared<StorageExternalDistributed>(
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
