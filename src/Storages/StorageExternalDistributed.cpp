#include <Storages/StorageExternalDistributed.h>

#include <Core/Settings.h>
#include <Storages/StorageFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Core/PostgreSQL/PoolWithFailover.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/parseAddress.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/parseRemoteDescription.h>
#include <Storages/StorageMySQL.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/StoragePostgreSQL.h>
#include <Storages/StorageURL.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/logger_useful.h>
#include <Processors/QueryPlan/UnionStep.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 glob_expansion_max_elements;
    extern const SettingsUInt64 postgresql_connection_attempt_timeout;
    extern const SettingsBool postgresql_connection_pool_auto_close_connection;
    extern const SettingsUInt64 postgresql_connection_pool_retries;
    extern const SettingsUInt64 postgresql_connection_pool_size;
    extern const SettingsUInt64 postgresql_connection_pool_wait_timeout;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

StorageExternalDistributed::StorageExternalDistributed(
    const StorageID & table_id_,
    std::unordered_set<StoragePtr> && shards_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage(table_id_)
    , shards(shards_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

void StorageExternalDistributed::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
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
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info);
    }

    if (plans.size() == 1)
    {
        query_plan = std::move(*plans.front());
        return;
    }

    Headers input_headers;
    input_headers.reserve(plans.size());
    for (auto & plan : plans)
        input_headers.emplace_back(plan->getCurrentHeader());

    auto union_step = std::make_unique<UnionStep>(std::move(input_headers));
    query_plan.unitePlans(std::move(union_step), std::move(plans));
}

void registerStorageExternalDistributed(StorageFactory & factory)
{
    factory.registerStorage("ExternalDistributed", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        if (engine_args.size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Engine ExternalDistributed must have at least 2 arguments: "
                            "engine_name, named_collection and/or description");

        auto context = args.getLocalContext();
        const auto & settings = context->getSettingsRef();
        size_t max_addresses = settings[Setting::glob_expansion_max_elements];
        auto get_addresses = [&](const std::string addresses_expr)
        {
            return parseRemoteDescription(addresses_expr, 0, addresses_expr.size(), ',', max_addresses);
        };

        std::unordered_set<StoragePtr> shards;
        ASTs inner_engine_args(engine_args.begin() + 1, engine_args.end());

        ASTPtr * address_arg = nullptr;

        /// If there is a named collection argument, named `addresses_expr`
        for (auto & node : inner_engine_args)
        {
            if (ASTFunction * func = node->as<ASTFunction>(); func && func->name == "equals" && func->arguments)
            {
                if (ASTExpressionList * func_args = func->arguments->as<ASTExpressionList>(); func_args && func_args->children.size() == 2)
                {
                    if (ASTIdentifier * arg_name = func_args->children[0]->as<ASTIdentifier>(); arg_name && arg_name->name() == "addresses_expr")
                    {
                        address_arg = &func_args->children[1];
                        break;
                    }
                }
            }
        }

        /// Otherwise it is the first argument.
        if (!address_arg)
            address_arg = &inner_engine_args.at(0);

        String addresses_expr = checkAndGetLiteralArgument<String>(*address_arg, "addresses");
        Strings shards_addresses = get_addresses(addresses_expr);

        auto engine_name = checkAndGetLiteralArgument<String>(engine_args[0], "engine_name");
        if (engine_name == "URL")
        {
            auto format_settings = StorageURL::getFormatSettingsFromArgs(args);
            for (const auto & shard_address : shards_addresses)
            {
                *address_arg = std::make_shared<ASTLiteral>(shard_address);
                auto configuration = StorageURL::getConfiguration(inner_engine_args, context);
                auto uri_options = parseRemoteDescription(shard_address, 0, shard_address.size(), '|', max_addresses);
                if (uri_options.size() > 1)
                {
                    shards.insert(
                        std::make_shared<StorageURLWithFailover>(
                            uri_options, args.table_id, configuration.format, format_settings,
                            args.columns, args.constraints, context, configuration.compression_method));
                }
                else
                {
                    shards.insert(std::make_shared<StorageURL>(
                        shard_address, args.table_id, configuration.format, format_settings,
                        args.columns, args.constraints, String{}, context, configuration.compression_method));
                }
            }
        }
#if USE_MYSQL
        else if (engine_name == "MySQL")
        {
            MySQLSettings mysql_settings;
            for (const auto & shard_address : shards_addresses)
            {
                *address_arg = std::make_shared<ASTLiteral>(shard_address);
                auto configuration = StorageMySQL::getConfiguration(inner_engine_args, context, mysql_settings);
                configuration.addresses = parseRemoteDescriptionForExternalDatabase(shard_address, max_addresses, 3306);
                auto pool = createMySQLPoolWithFailover(configuration, mysql_settings);
                shards.insert(std::make_shared<StorageMySQL>(
                    args.table_id, std::move(pool), configuration.database, configuration.table,
                    /* replace_query = */ false, /* on_duplicate_clause = */ "",
                    args.columns, args.constraints, String{}, context, mysql_settings));
            }
        }
#endif
#if USE_LIBPQXX
        else if (engine_name == "PostgreSQL")
        {
            for (const auto & shard_address : shards_addresses)
            {
                *address_arg = std::make_shared<ASTLiteral>(shard_address);
                auto configuration = StoragePostgreSQL::getConfiguration(inner_engine_args, context);
                configuration.addresses = parseRemoteDescriptionForExternalDatabase(shard_address, max_addresses, 5432);
                auto pool = std::make_shared<postgres::PoolWithFailover>(
                    configuration,
                    settings[Setting::postgresql_connection_pool_size],
                    settings[Setting::postgresql_connection_pool_wait_timeout],
                    settings[Setting::postgresql_connection_pool_retries],
                    settings[Setting::postgresql_connection_pool_auto_close_connection],
                    settings[Setting::postgresql_connection_attempt_timeout]);
                shards.insert(std::make_shared<StoragePostgreSQL>(
                    args.table_id, std::move(pool), configuration.table, args.columns, args.constraints, String{}, context));
            }
        }
#endif
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "External storage engine {} is not supported for StorageExternalDistributed. "
                "Supported engines are: MySQL, PostgreSQL, URL",
                engine_name);
        }

        return std::make_shared<StorageExternalDistributed>(
            args.table_id,
            std::move(shards),
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessType::SOURCES,
    });
}

}
