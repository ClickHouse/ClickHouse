#include "config.h"

#if USE_MYSQL

#    include <Databases/MySQL/DatabaseMaterializedMySQL.h>

#    include <Interpreters/Context.h>
#    include <Databases/MySQL/DatabaseMaterializedTablesIterator.h>
#    include <Databases/MySQL/MaterializedMySQLSyncThread.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Storages/StorageMaterializedMySQL.h>
#    include <Common/setThreadName.h>
#    include <Common/PoolId.h>
#    include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

DatabaseMaterializedMySQL::DatabaseMaterializedMySQL(
    ContextPtr context_,
    const String & database_name_,
    const String & metadata_path_,
    UUID uuid,
    const String & mysql_database_name_,
    mysqlxx::Pool && pool_,
    MySQLClient && client_,
    std::unique_ptr<MaterializedMySQLSettings> settings_)
    : DatabaseAtomic(database_name_, metadata_path_, uuid, "DatabaseMaterializedMySQL(" + database_name_ + ")", context_)
    , settings(std::move(settings_))
    , materialize_thread(context_, database_name_, mysql_database_name_, std::move(pool_), std::move(client_), settings.get())
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
            if (mode < LoadingStrictnessLevel::FORCE_ATTACH)
                materialize_thread.assertMySQLAvailable();

            materialize_thread.startSynchronization();
            started_up = true;
        });
    return startup_mysql_database_task = makeLoadTask(async_loader, {job});
}

void DatabaseMaterializedMySQL::waitDatabaseStarted(bool no_throw) const
{
    if (startup_mysql_database_task)
        waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), startup_mysql_database_task, no_throw);
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

void DatabaseMaterializedMySQL::attachTableUnlocked(ContextPtr context_, const String & name, const StoragePtr & table, const String & relative_table_path)
{
    checkIsInternalQuery(context_, "ATTACH TABLE");
    DatabaseAtomic::attachTableUnlocked(context_, name, table, relative_table_path);
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
        fs::remove(metadata);

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
DatabaseMaterializedMySQL::getTablesIterator(ContextPtr context_, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name) const
{
    DatabaseTablesIteratorPtr iterator = DatabaseAtomic::getTablesIterator(context_, filter_by_table_name);
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
    waitDatabaseStarted(/* no_throw = */ true);
    materialize_thread.stopSynchronization();
    started_up = false;
}

}

#endif
