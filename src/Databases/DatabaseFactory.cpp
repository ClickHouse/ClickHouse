#include <Databases/DatabaseFactory.h>

#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseDictionary.h>
#include <Databases/DatabaseLazy.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/formatAST.h>
#include <Common/Macros.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <filesystem>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#    include <Core/MySQL/MySQLClient.h>
#    include <Databases/MySQL/ConnectionMySQLSettings.h>
#    include <Databases/MySQL/DatabaseMySQL.h>
#    include <Databases/MySQL/MaterializedMySQLSettings.h>
#    include <Databases/MySQL/DatabaseMaterializedMySQL.h>
#    include <mysqlxx/Pool.h>
#endif

#if USE_MYSQL || USE_LIBPQXX
#include <Common/parseRemoteDescription.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Common/parseAddress.h>
#endif

#if USE_LIBPQXX
#include <Databases/PostgreSQL/DatabasePostgreSQL.h> // Y_IGNORE
#include <Databases/PostgreSQL/DatabaseMaterializedPostgreSQL.h>
#include <Storages/PostgreSQL/MaterializedPostgreSQLSettings.h>
#include <Storages/StoragePostgreSQL.h>
#endif

#if USE_SQLITE
#include <Databases/SQLite/DatabaseSQLite.h>
#endif

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int CANNOT_CREATE_DATABASE;
}

DatabasePtr DatabaseFactory::get(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context)
{
    bool created = false;

    try
    {
        /// Creates store/xxx/ for Atomic
        fs::create_directories(fs::path(metadata_path).parent_path());

        /// Before 20.7 it's possible that .sql metadata file does not exist for some old database.
        /// In this case Ordinary database is created on server startup if the corresponding metadata directory exists.
        /// So we should remove metadata directory if database creation failed.
        /// TODO remove this code
        created = fs::create_directory(metadata_path);

        DatabasePtr impl = getImpl(create, metadata_path, context);

        if (impl && context->hasQueryContext() && context->getSettingsRef().log_queries)
            context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Database, impl->getEngineName());

        // Attach database metadata
        if (impl && create.comment)
            impl->setDatabaseComment(create.comment->as<ASTLiteral>()->value.safeGet<String>());

        return impl;
    }
    catch (...)
    {
        if (created && fs::exists(metadata_path))
            fs::remove_all(metadata_path);
        throw;
    }
}

template <typename ValueType>
static inline ValueType safeGetLiteralValue(const ASTPtr &ast, const String &engine_name)
{
    if (!ast || !ast->as<ASTLiteral>())
        throw Exception("Database engine " + engine_name + " requested literal argument.", ErrorCodes::BAD_ARGUMENTS);

    return ast->as<ASTLiteral>()->value.safeGet<ValueType>();
}

DatabasePtr DatabaseFactory::getImpl(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context)
{
    auto * engine_define = create.storage;
    const String & database_name = create.database;
    const String & engine_name = engine_define->engine->name;
    const UUID & uuid = create.uuid;

    static const std::unordered_set<std::string_view> database_engines{"Ordinary", "Atomic", "Memory",
        "Dictionary", "Lazy", "Replicated", "MySQL", "MaterializeMySQL", "MaterializedMySQL",
        "PostgreSQL", "MaterializedPostgreSQL", "SQLite"};

    if (!database_engines.contains(engine_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine name `{}` does not exist", engine_name);

    static const std::unordered_set<std::string_view> engines_with_arguments{"MySQL", "MaterializeMySQL", "MaterializedMySQL",
        "Lazy", "Replicated", "PostgreSQL", "MaterializedPostgreSQL", "SQLite"};

    bool engine_may_have_arguments = engines_with_arguments.contains(engine_name);

    if (engine_define->engine->arguments && !engine_may_have_arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine `{}` cannot have arguments", engine_name);

    bool has_unexpected_element = engine_define->engine->parameters || engine_define->partition_by ||
                                  engine_define->primary_key || engine_define->order_by ||
                                  engine_define->sample_by;
    bool may_have_settings = endsWith(engine_name, "MySQL") || engine_name == "Replicated" || engine_name == "MaterializedPostgreSQL";

    if (has_unexpected_element || (!may_have_settings && engine_define->settings))
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_AST,
                        "Database engine `{}` cannot have parameters, primary_key, order_by, sample_by, settings", engine_name);

    if (engine_name == "Ordinary")
        return std::make_shared<DatabaseOrdinary>(database_name, metadata_path, context);
    else if (engine_name == "Atomic")
        return std::make_shared<DatabaseAtomic>(database_name, metadata_path, uuid, context);
    else if (engine_name == "Memory")
        return std::make_shared<DatabaseMemory>(database_name, context);
    else if (engine_name == "Dictionary")
        return std::make_shared<DatabaseDictionary>(database_name, context);

#if USE_MYSQL

    else if (engine_name == "MySQL" || engine_name == "MaterializeMySQL" || engine_name == "MaterializedMySQL")
    {
        const ASTFunction * engine = engine_define->engine;
        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);

        StorageMySQLConfiguration configuration;
        ASTs & arguments = engine->arguments->children;

        if (auto named_collection = getExternalDataSourceConfiguration(arguments, context, true))
        {
            auto [common_configuration, storage_specific_args] = named_collection.value();

            configuration.set(common_configuration);
            configuration.addresses = {std::make_pair(configuration.host, configuration.port)};

            if (!storage_specific_args.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "MySQL database require mysql_hostname, mysql_database_name, mysql_username, mysql_password arguments.");
        }
        else
        {
            if (arguments.size() != 4)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "MySQL database require mysql_hostname, mysql_database_name, mysql_username, mysql_password arguments.");


            arguments[1] = evaluateConstantExpressionOrIdentifierAsLiteral(arguments[1], context);
            const auto & host_port = safeGetLiteralValue<String>(arguments[0], engine_name);

            if (engine_name == "MySQL")
            {
                size_t max_addresses = context->getSettingsRef().glob_expansion_max_elements;
                configuration.addresses = parseRemoteDescriptionForExternalDatabase(host_port, max_addresses, 3306);
            }
            else
            {
                const auto & [remote_host, remote_port] = parseAddress(host_port, 3306);
                configuration.host = remote_host;
                configuration.port = remote_port;
            }

            configuration.database = safeGetLiteralValue<String>(arguments[1], engine_name);
            configuration.username = safeGetLiteralValue<String>(arguments[2], engine_name);
            configuration.password = safeGetLiteralValue<String>(arguments[3], engine_name);
        }

        try
        {
            if (engine_name == "MySQL")
            {
                auto mysql_database_settings = std::make_unique<ConnectionMySQLSettings>();
                auto mysql_pool = mysqlxx::PoolWithFailover(configuration.database, configuration.addresses, configuration.username, configuration.password);

                mysql_database_settings->loadFromQueryContext(context);
                mysql_database_settings->loadFromQuery(*engine_define); /// higher priority

                return std::make_shared<DatabaseMySQL>(
                    context, database_name, metadata_path, engine_define, configuration.database, std::move(mysql_database_settings), std::move(mysql_pool));
            }

            MySQLClient client(configuration.host, configuration.port, configuration.username, configuration.password);
            auto mysql_pool = mysqlxx::Pool(configuration.database, configuration.host, configuration.username, configuration.password, configuration.port);

            auto materialize_mode_settings = std::make_unique<MaterializedMySQLSettings>();

            if (engine_define->settings)
                materialize_mode_settings->loadFromQuery(*engine_define);

            if (create.uuid == UUIDHelpers::Nil)
                return std::make_shared<DatabaseMaterializedMySQL<DatabaseOrdinary>>(
                    context, database_name, metadata_path, uuid, configuration.database, std::move(mysql_pool),
                    std::move(client), std::move(materialize_mode_settings));
            else
                return std::make_shared<DatabaseMaterializedMySQL<DatabaseAtomic>>(
                    context, database_name, metadata_path, uuid, configuration.database, std::move(mysql_pool),
                    std::move(client), std::move(materialize_mode_settings));
        }
        catch (...)
        {
            const auto & exception_message = getCurrentExceptionMessage(true);
            throw Exception("Cannot create MySQL database, because " + exception_message, ErrorCodes::CANNOT_CREATE_DATABASE);
        }
    }
#endif

    else if (engine_name == "Lazy")
    {
        const ASTFunction * engine = engine_define->engine;

        if (!engine->arguments || engine->arguments->children.size() != 1)
            throw Exception("Lazy database require cache_expiration_time_seconds argument", ErrorCodes::BAD_ARGUMENTS);

        const auto & arguments = engine->arguments->children;

        const auto cache_expiration_time_seconds = safeGetLiteralValue<UInt64>(arguments[0], "Lazy");
        return std::make_shared<DatabaseLazy>(database_name, metadata_path, cache_expiration_time_seconds, context);
    }

    else if (engine_name == "Replicated")
    {
        const ASTFunction * engine = engine_define->engine;

        if (!engine->arguments || engine->arguments->children.size() != 3)
            throw Exception("Replicated database requires 3 arguments: zookeeper path, shard name and replica name", ErrorCodes::BAD_ARGUMENTS);

        const auto & arguments = engine->arguments->children;

        String zookeeper_path = safeGetLiteralValue<String>(arguments[0], "Replicated");
        String shard_name = safeGetLiteralValue<String>(arguments[1], "Replicated");
        String replica_name  = safeGetLiteralValue<String>(arguments[2], "Replicated");

        zookeeper_path = context->getMacros()->expand(zookeeper_path);
        shard_name = context->getMacros()->expand(shard_name);
        replica_name = context->getMacros()->expand(replica_name);

        DatabaseReplicatedSettings database_replicated_settings{};
        if (engine_define->settings)
            database_replicated_settings.loadFromQuery(*engine_define);

        return std::make_shared<DatabaseReplicated>(database_name, metadata_path, uuid,
                                                    zookeeper_path, shard_name, replica_name,
                                                    std::move(database_replicated_settings), context);
    }

#if USE_LIBPQXX

    else if (engine_name == "PostgreSQL")
    {
        const ASTFunction * engine = engine_define->engine;
        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);

        ASTs & engine_args = engine->arguments->children;
        auto use_table_cache = false;
        StoragePostgreSQLConfiguration configuration;

        if (auto named_collection = getExternalDataSourceConfiguration(engine_args, context, true))
        {
            auto [common_configuration, storage_specific_args] = named_collection.value();

            configuration.set(common_configuration);
            configuration.addresses = {std::make_pair(configuration.host, configuration.port)};

            for (const auto & [arg_name, arg_value] : storage_specific_args)
            {
                if (arg_name == "use_table_cache")
                    use_table_cache = true;
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Unexpected key-value argument."
                            "Got: {}, but expected one of:"
                            "host, port, username, password, database, schema, use_table_cache.", arg_name);
            }
        }
        else
        {
            if (engine_args.size() < 4 || engine_args.size() > 6)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "PostgreSQL Database require `host:port`, `database_name`, `username`, `password`"
                                "[, `schema` = "", `use_table_cache` = 0");

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

            const auto & host_port = safeGetLiteralValue<String>(engine_args[0], engine_name);
            size_t max_addresses = context->getSettingsRef().glob_expansion_max_elements;

            configuration.addresses = parseRemoteDescriptionForExternalDatabase(host_port, max_addresses, 5432);
            configuration.database = safeGetLiteralValue<String>(engine_args[1], engine_name);
            configuration.username = safeGetLiteralValue<String>(engine_args[2], engine_name);
            configuration.password = safeGetLiteralValue<String>(engine_args[3], engine_name);

            if (engine_args.size() >= 5)
                configuration.schema = safeGetLiteralValue<String>(engine_args[4], engine_name);
        }

        if (engine_args.size() >= 6)
            use_table_cache = safeGetLiteralValue<UInt8>(engine_args[5], engine_name);

        auto pool = std::make_shared<postgres::PoolWithFailover>(configuration,
            context->getSettingsRef().postgresql_connection_pool_size,
            context->getSettingsRef().postgresql_connection_pool_wait_timeout);

        return std::make_shared<DatabasePostgreSQL>(
            context, metadata_path, engine_define, database_name, configuration, pool, use_table_cache);
    }
    else if (engine_name == "MaterializedPostgreSQL")
    {
        const ASTFunction * engine = engine_define->engine;
        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);

        ASTs & engine_args = engine->arguments->children;
        StoragePostgreSQLConfiguration configuration;

        if (auto named_collection = getExternalDataSourceConfiguration(engine_args, context, true))
        {
            auto [common_configuration, storage_specific_args] = named_collection.value();
            configuration.set(common_configuration);

            if (!storage_specific_args.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "MaterializedPostgreSQL Database requires only `host`, `port`, `database_name`, `username`, `password`.");
        }
        else
        {
            if (engine_args.size() != 4)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "MaterializedPostgreSQL Database require `host:port`, `database_name`, `username`, `password`.");

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

            auto parsed_host_port = parseAddress(safeGetLiteralValue<String>(engine_args[0], engine_name), 5432);

            configuration.host = parsed_host_port.first;
            configuration.port = parsed_host_port.second;
            configuration.database = safeGetLiteralValue<String>(engine_args[1], engine_name);
            configuration.username = safeGetLiteralValue<String>(engine_args[2], engine_name);
            configuration.password = safeGetLiteralValue<String>(engine_args[3], engine_name);
        }

        auto connection_info = postgres::formatConnectionString(
            configuration.database, configuration.host, configuration.port, configuration.username, configuration.password);

        auto postgresql_replica_settings = std::make_unique<MaterializedPostgreSQLSettings>();
        if (engine_define->settings)
            postgresql_replica_settings->loadFromQuery(*engine_define);

        return std::make_shared<DatabaseMaterializedPostgreSQL>(
                context, metadata_path, uuid, create.attach,
                database_name, configuration.database, connection_info,
                std::move(postgresql_replica_settings));
    }


#endif

#if USE_SQLITE
    else if (engine_name == "SQLite")
    {
        const ASTFunction * engine = engine_define->engine;

        if (!engine->arguments || engine->arguments->children.size() != 1)
            throw Exception("SQLite database requires 1 argument: database path", ErrorCodes::BAD_ARGUMENTS);

        const auto & arguments = engine->arguments->children;

        String database_path = safeGetLiteralValue<String>(arguments[0], "SQLite");

        return std::make_shared<DatabaseSQLite>(context, engine_define, create.attach, database_path);
    }
#endif

    throw Exception("Unknown database engine: " + engine_name, ErrorCodes::UNKNOWN_DATABASE_ENGINE);
}

}
