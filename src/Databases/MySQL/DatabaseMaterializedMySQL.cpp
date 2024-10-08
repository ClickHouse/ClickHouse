#include "config.h"

#if USE_MYSQL

#    include <Core/Settings.h>
#    include <Databases/MySQL/DatabaseMaterializedMySQL.h>
#    include <Common/parseAddress.h>
#    include <Common/parseRemoteDescription.h>

#    include <Interpreters/evaluateConstantExpression.h>
#    include <Databases/DatabaseFactory.h>
#    include <Databases/MySQL/DatabaseMaterializedTablesIterator.h>
#    include <Databases/MySQL/MaterializedMySQLSyncThread.h>
#    include <Databases/MySQL/MySQLBinlogClientFactory.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Parsers/ASTFunction.h>
#    include <Parsers/queryToString.h>
#    include <Storages/StorageMySQL.h>
#    include <Storages/StorageMaterializedMySQL.h>
#    include <Storages/NamedCollectionsHelpers.h>
#    include <Common/setThreadName.h>
#    include <Common/PoolId.h>
#    include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 glob_expansion_max_elements;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

DatabaseMaterializedMySQL::DatabaseMaterializedMySQL(
    ContextPtr context_,
    const String & database_name_,
    const String & metadata_path_,
    UUID uuid,
    const String & mysql_database_name_,
    mysqlxx::Pool && pool_,
    MySQLClient && client_,
    const MySQLReplication::BinlogClientPtr & binlog_client_,
    std::unique_ptr<MaterializedMySQLSettings> settings_)
    : DatabaseAtomic(database_name_, metadata_path_, uuid, "DatabaseMaterializedMySQL(" + database_name_ + ")", context_)
    , settings(std::move(settings_))
    , materialize_thread(context_, database_name_, mysql_database_name_, std::move(pool_), std::move(client_), binlog_client_, settings.get())
{
}

void DatabaseMaterializedMySQL::rethrowExceptionIfNeeded() const
{
    std::lock_guard lock(mutex);

    if (!settings->allows_query_when_mysql_lost && exception)
    {
        try
        {
            std::rethrow_exception(exception);
        }
        catch (Exception & ex)
        {
            /// This method can be called from multiple threads
            /// and Exception can be modified concurrently by calling addMessage(...),
            /// so we rethrow a copy.
            throw Exception(ex);
        }
    }
}

void DatabaseMaterializedMySQL::setException(const std::exception_ptr & exception_)
{
    std::lock_guard lock(mutex);
    exception = exception_;
}

LoadTaskPtr DatabaseMaterializedMySQL::startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode)
{
    auto base = DatabaseAtomic::startupDatabaseAsync(async_loader, std::move(startup_after), mode);
    auto job = makeLoadJob(
        base->goals(),
        TablesLoaderBackgroundStartupPoolId,
        fmt::format("startup MaterializedMySQL database {}", getDatabaseName()),
        [this, mode] (AsyncLoader &, const LoadJobPtr &)
        {
            LOG_TRACE(log, "Starting MaterializeMySQL database");
            if (!settings->allow_startup_database_without_connection_to_mysql
                && mode < LoadingStrictnessLevel::FORCE_ATTACH)
                materialize_thread.assertMySQLAvailable();

            materialize_thread.startSynchronization();
            started_up = true;
        });
    std::scoped_lock lock(mutex);
    return startup_mysql_database_task = makeLoadTask(async_loader, {job});
}

void DatabaseMaterializedMySQL::waitDatabaseStarted() const
{
    LoadTaskPtr task;
    {
        std::scoped_lock lock(mutex);
        task = startup_mysql_database_task;
    }
    if (task)
        waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), task);
}

void DatabaseMaterializedMySQL::stopLoading()
{
    LoadTaskPtr stop_startup_mysql_database;
    {
        std::scoped_lock lock(mutex);
        stop_startup_mysql_database.swap(startup_mysql_database_task);
    }
    stop_startup_mysql_database.reset();
    DatabaseAtomic::stopLoading();
}

void DatabaseMaterializedMySQL::createTable(ContextPtr context_, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    checkIsInternalQuery(context_, "CREATE TABLE");
    DatabaseAtomic::createTable(context_, name, table, query);
}

void DatabaseMaterializedMySQL::dropTable(ContextPtr context_, const String & name, bool sync)
{
    checkIsInternalQuery(context_, "DROP TABLE");
    DatabaseAtomic::dropTable(context_, name, sync);
}

void DatabaseMaterializedMySQL::attachTable(ContextPtr context_, const String & name, const StoragePtr & table, const String & relative_table_path)
{
    checkIsInternalQuery(context_, "ATTACH TABLE");
    DatabaseAtomic::attachTable(context_, name, table, relative_table_path);
}

StoragePtr DatabaseMaterializedMySQL::detachTable(ContextPtr context_, const String & name)
{
    checkIsInternalQuery(context_, "DETACH TABLE");
    return DatabaseAtomic::detachTable(context_, name);
}

void DatabaseMaterializedMySQL::renameTable(ContextPtr context_, const String & name, IDatabase & to_database, const String & to_name, bool exchange, bool dictionary)
{
    checkIsInternalQuery(context_, "RENAME TABLE");

    if (exchange)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MaterializedMySQL database does not support EXCHANGE TABLE.");

    if (dictionary)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MaterializedMySQL database does not support RENAME DICTIONARY.");

    if (to_database.getDatabaseName() != DatabaseAtomic::getDatabaseName())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot rename with other database for MaterializedMySQL database.");

    DatabaseAtomic::renameTable(context_, name, *this, to_name, exchange, dictionary);
}

void DatabaseMaterializedMySQL::alterTable(ContextPtr context_, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    checkIsInternalQuery(context_, "ALTER TABLE");
    DatabaseAtomic::alterTable(context_, table_id, metadata);
}

void DatabaseMaterializedMySQL::drop(ContextPtr context_)
{
    LOG_TRACE(log, "Dropping MaterializeMySQL database");
    /// Remove metadata info
    fs::path metadata(getMetadataPath() + "/.metadata");

    if (fs::exists(metadata))
        (void)fs::remove(metadata);

    DatabaseAtomic::drop(context_);
}

StoragePtr DatabaseMaterializedMySQL::tryGetTable(const String & name, ContextPtr context_) const
{
    StoragePtr nested_storage = DatabaseAtomic::tryGetTable(name, context_);
    if (context_->isInternalQuery())
        return nested_storage;
    if (nested_storage)
        return std::make_shared<StorageMaterializedMySQL>(std::move(nested_storage), this);
    return nullptr;
}

DatabaseTablesIteratorPtr
DatabaseMaterializedMySQL::getTablesIterator(ContextPtr context_, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    DatabaseTablesIteratorPtr iterator = DatabaseAtomic::getTablesIterator(context_, filter_by_table_name, skip_not_loaded);
    if (context_->isInternalQuery())
        return iterator;
    return std::make_unique<DatabaseMaterializedTablesIterator>(std::move(iterator), this);
}

void DatabaseMaterializedMySQL::checkIsInternalQuery(ContextPtr context_, const char * method) const
{
    if (started_up && context_ && !context_->isInternalQuery())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MaterializedMySQL database does not support {}", method);
}

void DatabaseMaterializedMySQL::stopReplication()
{
    materialize_thread.stopSynchronization();
    started_up = false;
}

void registerDatabaseMaterializedMySQL(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;
        const String & engine_name = engine_define->engine->name;

        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);
        MySQLReplication::BinlogClientPtr binlog_client;
        StorageMySQL::Configuration configuration;
        ASTs & arguments = engine->arguments->children;
        auto mysql_settings = std::make_unique<MySQLSettings>();

        if (auto named_collection = tryGetNamedCollectionWithOverrides(arguments, args.context))
        {
            configuration = StorageMySQL::processNamedCollectionResult(*named_collection, *mysql_settings, args.context, false);
        }
        else
        {
            if (arguments.size() != 4)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "MySQL database require mysql_hostname, mysql_database_name, mysql_username, mysql_password arguments.");


            arguments[1] = evaluateConstantExpressionOrIdentifierAsLiteral(arguments[1], args.context);
            const auto & host_port = safeGetLiteralValue<String>(arguments[0], engine_name);

            if (engine_name == "MySQL")
            {
                size_t max_addresses = args.context->getSettingsRef()[Setting::glob_expansion_max_elements];
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
        MySQLClient client(configuration.host, configuration.port, configuration.username, configuration.password);
        auto mysql_pool
            = mysqlxx::Pool(configuration.database, configuration.host, configuration.username, configuration.password, configuration.port);

        auto materialize_mode_settings = std::make_unique<MaterializedMySQLSettings>();

        if (engine_define->settings)
            materialize_mode_settings->loadFromQuery(*engine_define);

        if (materialize_mode_settings->use_binlog_client)
            binlog_client = DB::MySQLReplication::BinlogClientFactory::instance().getClient(
                configuration.host, configuration.port, configuration.username, configuration.password,
                materialize_mode_settings->max_bytes_in_binlog_dispatcher_buffer,
                materialize_mode_settings->max_flush_milliseconds_in_binlog_dispatcher);

        if (args.uuid == UUIDHelpers::Nil)
        {
            auto print_create_ast = args.create_query.clone();
            print_create_ast->as<ASTCreateQuery>()->attach = false;
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The MaterializedMySQL database engine no longer supports Ordinary databases. To re-create the database, delete "
                "the old one by executing \"rm -rf {}{{,.sql}}\", then re-create the database with the following query: {}",
                args.metadata_path,
                queryToString(print_create_ast));
        }

        return make_shared<DatabaseMaterializedMySQL>(
            args.context,
            args.database_name,
            args.metadata_path,
            args.uuid,
            configuration.database,
            std::move(mysql_pool),
            std::move(client),
            binlog_client,
            std::move(materialize_mode_settings));
    };

    DatabaseFactory::EngineFeatures features{
        .supports_arguments = true,
        .supports_settings = true,
        .supports_table_overrides = true,
    };
    factory.registerDatabase("MaterializeMySQL", create_fn, features);
    factory.registerDatabase("MaterializedMySQL", create_fn, features);
}

}

#endif
