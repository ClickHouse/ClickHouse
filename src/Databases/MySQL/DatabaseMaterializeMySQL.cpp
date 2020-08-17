#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#    include <Databases/MySQL/DatabaseMaterializeMySQL.h>

#    include <Interpreters/Context.h>
#    include <Databases/DatabaseOrdinary.h>
#    include <Databases/MySQL/DatabaseMaterializeTablesIterator.h>
#    include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Storages/StorageMaterializeMySQL.h>
#    include <Poco/File.h>
#    include <Poco/Logger.h>
#    include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

DatabaseMaterializeMySQL::DatabaseMaterializeMySQL(
    const Context & context, const String & database_name_, const String & metadata_path_, const IAST * database_engine_define_
    , const String & mysql_database_name_, mysqlxx::Pool && pool_, MySQLClient && client_, std::unique_ptr<MaterializeMySQLSettings> settings_)
    : IDatabase(database_name_), global_context(context.getGlobalContext()), engine_define(database_engine_define_->clone())
    , nested_database(std::make_shared<DatabaseOrdinary>(database_name_, metadata_path_, context))
    , settings(std::move(settings_)), log(&Poco::Logger::get("DatabaseMaterializeMySQL"))
    , materialize_thread(context, database_name_, mysql_database_name_, std::move(pool_), std::move(client_), settings.get())
{
}

void DatabaseMaterializeMySQL::rethrowExceptionIfNeed() const
{
    std::unique_lock<std::mutex> lock(mutex);

    if (!settings->allows_query_when_mysql_lost && exception)
    {
        try
        {
            std::rethrow_exception(exception);
        }
        catch (Exception & ex)
        {
            throw Exception(ex);
        }
    }
}

void DatabaseMaterializeMySQL::setException(const std::exception_ptr & exception_)
{
    std::unique_lock<std::mutex> lock(mutex);
    exception = exception_;
}

ASTPtr DatabaseMaterializeMySQL::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = database_name;
    create_query->set(create_query->storage, engine_define);
    return create_query;
}

void DatabaseMaterializeMySQL::loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach)
{
    try
    {
        std::unique_lock<std::mutex> lock(mutex);
        nested_database->loadStoredObjects(context, has_force_restore_data_flag, force_attach);
        materialize_thread.startSynchronization();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot load MySQL nested database stored objects.");

        if (!force_attach)
            throw;
    }
}

void DatabaseMaterializeMySQL::shutdown()
{
    materialize_thread.stopSynchronization();

    auto iterator = nested_database->getTablesIterator(global_context, {});

    /// We only shutdown the table, The tables is cleaned up when destructed database
    for (; iterator->isValid(); iterator->next())
        iterator->table()->shutdown();
}

bool DatabaseMaterializeMySQL::empty() const
{
    return nested_database->empty();
}

String DatabaseMaterializeMySQL::getDataPath() const
{
    return nested_database->getDataPath();
}

String DatabaseMaterializeMySQL::getMetadataPath() const
{
    return nested_database->getMetadataPath();
}

String DatabaseMaterializeMySQL::getTableDataPath(const String & table_name) const
{
    return nested_database->getTableDataPath(table_name);
}

String DatabaseMaterializeMySQL::getTableDataPath(const ASTCreateQuery & query) const
{
    return nested_database->getTableDataPath(query);
}

String DatabaseMaterializeMySQL::getObjectMetadataPath(const String & table_name) const
{
    return nested_database->getObjectMetadataPath(table_name);
}

UUID DatabaseMaterializeMySQL::tryGetTableUUID(const String & table_name) const
{
    return nested_database->tryGetTableUUID(table_name);
}

time_t DatabaseMaterializeMySQL::getObjectMetadataModificationTime(const String & name) const
{
    return nested_database->getObjectMetadataModificationTime(name);
}

void DatabaseMaterializeMySQL::createTable(const Context & context, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support create table.", ErrorCodes::NOT_IMPLEMENTED);

    nested_database->createTable(context, name, table, query);
}

void DatabaseMaterializeMySQL::dropTable(const Context & context, const String & name, bool no_delay)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support drop table.", ErrorCodes::NOT_IMPLEMENTED);

    nested_database->dropTable(context, name, no_delay);
}

void DatabaseMaterializeMySQL::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support attach table.", ErrorCodes::NOT_IMPLEMENTED);

    nested_database->attachTable(name, table, relative_table_path);
}

StoragePtr DatabaseMaterializeMySQL::detachTable(const String & name)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support detach table.", ErrorCodes::NOT_IMPLEMENTED);

    return nested_database->detachTable(name);
}

void DatabaseMaterializeMySQL::renameTable(const Context & context, const String & name, IDatabase & to_database, const String & to_name, bool exchange, bool dictionary)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support rename table.", ErrorCodes::NOT_IMPLEMENTED);

    if (exchange)
        throw Exception("MaterializeMySQL database not support exchange table.", ErrorCodes::NOT_IMPLEMENTED);

    if (dictionary)
        throw Exception("MaterializeMySQL database not support rename dictionary.", ErrorCodes::NOT_IMPLEMENTED);

    if (to_database.getDatabaseName() != getDatabaseName())
        throw Exception("Cannot rename with other database for MaterializeMySQL database.", ErrorCodes::NOT_IMPLEMENTED);

    nested_database->renameTable(context, name, *nested_database, to_name, exchange, dictionary);
}

void DatabaseMaterializeMySQL::alterTable(const Context & context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support alter table.", ErrorCodes::NOT_IMPLEMENTED);

    nested_database->alterTable(context, table_id, metadata);
}

bool DatabaseMaterializeMySQL::shouldBeEmptyOnDetach() const
{
    return false;
}

void DatabaseMaterializeMySQL::drop(const Context & context)
{
    if (nested_database->shouldBeEmptyOnDetach())
    {
        for (auto iterator = nested_database->getTablesIterator(context, {}); iterator->isValid(); iterator->next())
        {
            TableExclusiveLockHolder table_lock = iterator->table()->lockExclusively(
                context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);

            nested_database->dropTable(context, iterator->name(), true);
        }

        /// Remove metadata info
        Poco::File metadata(getMetadataPath() + "/.metadata");

        if (metadata.exists())
            metadata.remove(false);
    }

    nested_database->drop(context);
}

bool DatabaseMaterializeMySQL::isTableExist(const String & name, const Context & context) const
{
    return nested_database->isTableExist(name, context);
}

StoragePtr DatabaseMaterializeMySQL::tryGetTable(const String & name, const Context & context) const
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
    {
        StoragePtr nested_storage = nested_database->tryGetTable(name, context);

        if (!nested_storage)
            return {};

        return std::make_shared<StorageMaterializeMySQL>(std::move(nested_storage), this);
    }

    return nested_database->tryGetTable(name, context);
}

DatabaseTablesIteratorPtr DatabaseMaterializeMySQL::getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
    {
        DatabaseTablesIteratorPtr iterator = nested_database->getTablesIterator(context, filter_by_table_name);
        return std::make_unique<DatabaseMaterializeTablesIterator>(std::move(iterator), this);
    }

    return nested_database->getTablesIterator(context, filter_by_table_name);
}

}

#endif
