#include <Databases/DatabaseFactory.h>

#include <filesystem>
#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseDictionary.h>
#include <Databases/DatabaseLazy.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/queryToString.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Common/logger_useful.h>
#include <Common/Macros.h>

#include "config_core.h"

#if USE_MYSQL
#    include <Core/MySQL/MySQLClient.h>
#    include <Databases/MySQL/ConnectionMySQLSettings.h>
#    include <Databases/MySQL/DatabaseMySQL.h>
#    include <Databases/MySQL/MaterializedMySQLSettings.h>
#    include <Storages/MySQL/MySQLHelpers.h>
#    include <Storages/MySQL/MySQLSettings.h>
#    include <Databases/MySQL/DatabaseMaterializedMySQL.h>
#    include <mysqlxx/Pool.h>
#endif

#if USE_MYSQL || USE_LIBPQXX
#include <Common/parseRemoteDescription.h>
#include <Common/parseAddress.h>
#endif

#if USE_LIBPQXX
#include <Databases/PostgreSQL/DatabasePostgreSQL.h>
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
    extern const int NOT_IMPLEMENTED;
}

DatabasePtr DatabaseFactory::get(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context)
{
    /// Creates store/xxx/ for Atomic
    fs::create_directories(fs::path(metadata_path).parent_path());

    DatabasePtr impl = getImpl(create, metadata_path, context);

    if (impl && context->hasQueryContext() && context->getSettingsRef().log_queries)
        context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Database, impl->getEngineName());

    /// Attach database metadata
    if (impl && create.comment)
        impl->setDatabaseComment(create.comment->as<ASTLiteral>()->value.safeGet<String>());

    return impl;
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
    const String & database_name = create.getDatabase();
    const String & engine_name = engine_define->engine->name;
    const UUID & uuid = create.uuid;

    static const std::unordered_set<std::string_view> database_engines{"Ordinary", "Atomic", "Memory",
        "Dictionary", "Lazy", "Replicated", "MySQL", "MaterializeMySQL", "MaterializedMySQL",
        "PostgreSQL", "MaterializedPostgreSQL", "SQLite"};

    if (!database_engines.contains(engine_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine name `{}` does not exist", engine_name);

    static const std::unordered_set<std::string_view> engines_with_arguments{"MySQL", "MaterializeMySQL", "MaterializedMySQL",
        "Lazy", "Replicated", "PostgreSQL", "MaterializedPostgreSQL", "SQLite"};

    static const std::unordered_set<std::string_view> engines_with_table_overrides{"MaterializeMySQL", "MaterializedMySQL", "MaterializedPostgreSQL"};
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

    if (create.table_overrides && !engines_with_table_overrides.contains(engine_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine `{}` cannot have table overrides", engine_name);

    if (engine_name == "Ordinary")
    {
        if (!create.attach && !context->getSettingsRef().allow_deprecated_database_ordinary)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE_ENGINE,
                            "Ordinary database engine is deprecated (see also allow_deprecated_database_ordinary setting)");

        /// Before 20.7 metadata/db_name.sql file might absent and Ordinary database was attached if there's metadata/db_name/ dir.
        /// Between 20.7 and 22.7 metadata/db_name.sql was created in this case as well.
        /// Since 20.7 `default` database is created with Atomic engine on the very first server run.
        /// The problem is that if server crashed during the very first run and metadata/db_name/ -> store/whatever symlink was created
        /// then it's considered as Ordinary database. And it even works somehow
        /// until background task tries to remove onused dir from store/...
        if (fs::is_symlink(metadata_path))
            throw Exception(ErrorCodes::CANNOT_CREATE_DATABASE, "Metadata directory {} for Ordinary database {} is a symbolic link to {}. "
                            "It may be a result of manual intervention, crash on very first server start or a bug. "
                            "Database cannot be attached (it's kind of protection from potential data loss). "
                            "Metadata directory must not be a symlink and must contain tables metadata files itself. "
                            "You have to resolve this manually.",
                            metadata_path, database_name, fs::read_symlink(metadata_path).string());
        return std::make_shared<DatabaseOrdinary>(database_name, metadata_path, context);
    }

    if (engine_name == "Atomic")
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
        ASTList & arguments = engine->arguments->children;
        MySQLSettings mysql_settings;

        if (auto named_collection = getExternalDataSourceConfiguration(arguments, context, true, true, mysql_settings))
        {
            auto [common_configuration, storage_specific_args, settings_changes] = named_collection.value();

            configuration.set(common_configuration);
            configuration.addresses = {std::make_pair(configuration.host, configuration.port)};
            mysql_settings.applyChanges(settings_changes);

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
                auto mysql_pool = createMySQLPoolWithFailover(configuration, mysql_settings);

                mysql_database_settings->loadFromQueryContext(context);
                mysql_database_settings->loadFromQuery(*engine_define); /// higher priority

                return std::make_shared<DatabaseMySQL>(
                    context, database_name, metadata_path, engine_define, configuration.database,
                    std::move(mysql_database_settings), std::move(mysql_pool), create.attach);
            }

            MySQLClient client(configuration.host, configuration.port, configuration.username, configuration.password);
            auto mysql_pool = mysqlxx::Pool(configuration.database, configuration.host, configuration.username, configuration.password, configuration.port);

            auto materialize_mode_settings = std::make_unique<MaterializedMySQLSettings>();

            if (engine_define->settings)
                materialize_mode_settings->loadFromQuery(*engine_define);

            if (uuid == UUIDHelpers::Nil)
            {
                auto print_create_ast = create.clone();
                print_create_ast->as<ASTCreateQuery>()->attach = false;
                throw Exception(
                    fmt::format(
                        "The MaterializedMySQL database engine no longer supports Ordinary databases. To re-create the database, delete "
                        "the old one by executing \"rm -rf {}{{,.sql}}\", then re-create the database with the following query: {}",
                        metadata_path,
                        queryToString(print_create_ast)),
                    ErrorCodes::NOT_IMPLEMENTED);
            }

            return std::make_shared<DatabaseMaterializedMySQL>(
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

        const auto cache_expiration_time_seconds = safeGetLiteralValue<UInt64>(arguments.front(), "Lazy");
        return std::make_shared<DatabaseLazy>(database_name, metadata_path, cache_expiration_time_seconds, context);
    }

    else if (engine_name == "Replicated")
    {
        const ASTFunction * engine = engine_define->engine;

        if (!engine->arguments || engine->arguments->children.size() != 3)
            throw Exception("Replicated database requires 3 arguments: zookeeper path, shard name and replica name", ErrorCodes::BAD_ARGUMENTS);

        auto & arguments = engine->arguments->children;
        for (auto & engine_arg : arguments)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        String zookeeper_path = safeGetLiteralValue<String>(arguments.front(), "Replicated");
        String shard_name = safeGetLiteralValue<String>(*++arguments.begin(), "Replicated");
        String replica_name  = safeGetLiteralValue<String>(arguments.back(), "Replicated");

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

        AstList & engine_args = engine->arguments->children;
        auto use_table_cache = false;
        StoragePostgreSQLConfiguration configuration;

        if (auto named_collection = getExternalDataSourceConfiguration(engine_args, context, true))
        {
            auto [common_configuration, storage_specific_args, _] = named_collection.value();

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

            bool is_deprecated_syntax = false;
            if (engine_args.size() >= 5)
            {
                auto arg_value = engine_args[4]->as<ASTLiteral>()->value;
                if (arg_value.getType() == Field::Types::Which::String)
                {
                    configuration.schema = safeGetLiteralValue<String>(engine_args[4], engine_name);
                }
                else
                {
                    use_table_cache = safeGetLiteralValue<UInt8>(engine_args[4], engine_name);
                    LOG_WARNING(&Poco::Logger::get("DatabaseFactory"), "A deprecated syntax of PostgreSQL database engine is used");
                    is_deprecated_syntax = true;
                }
            }

            if (!is_deprecated_syntax && engine_args.size() >= 6)
                use_table_cache = safeGetLiteralValue<UInt8>(engine_args[5], engine_name);
        }

        const auto & settings = context->getSettingsRef();
        auto pool = std::make_shared<postgres::PoolWithFailover>(
            configuration,
            settings.postgresql_connection_pool_size,
            settings.postgresql_connection_pool_wait_timeout,
            POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES,
            settings.postgresql_connection_pool_auto_close_connection);

        return std::make_shared<DatabasePostgreSQL>(
            context, metadata_path, engine_define, database_name, configuration, pool, use_table_cache);
    }
    else if (engine_name == "MaterializedPostgreSQL")
    {
        const ASTFunction * engine = engine_define->engine;
        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);

        ASTList & engine_args = engine->arguments->children;
        StoragePostgreSQLConfiguration configuration;

        if (auto named_collection = getExternalDataSourceConfiguration(engine_args, context, true))
        {
            auto [common_configuration, storage_specific_args, _] = named_collection.value();
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

        String database_path = safeGetLiteralValue<String>(arguments.front(), "SQLite");

        return std::make_shared<DatabaseSQLite>(context, engine_define, create.attach, database_path);
    }
#endif

    throw Exception("Unknown database engine: " + engine_name, ErrorCodes::UNKNOWN_DATABASE_ENGINE);
}

}
