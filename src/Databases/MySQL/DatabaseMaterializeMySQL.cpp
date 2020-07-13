#include <Databases/MySQL/DatabaseMaterializeMySQL.h>

#include <Databases/DatabaseOrdinary.h>
#include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#include <Databases/MySQL/DatabaseMaterializeTablesIterator.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/StorageMaterializeMySQL.h>
#include <Poco/Logger.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

DatabaseMaterializeMySQL::DatabaseMaterializeMySQL(
    const Context & context, const String & database_name_, const String & metadata_path_, const IAST * database_engine_define_
    , const String & mysql_database_name_, mysqlxx::Pool && pool_, MySQLClient && client_, std::unique_ptr<MaterializeMySQLSettings> settings_)
    : IDatabase(database_name_), engine_define(database_engine_define_->clone())
    , nested_database(std::make_shared<DatabaseOrdinary>(database_name_, metadata_path_, context))
    , settings(std::move(settings_)), log(&Poco::Logger::get("DatabaseMaterializeMySQL"))
    , materialize_thread(context, database_name_, mysql_database_name_, std::move(pool_), std::move(client_), settings.get())
{
}

void DatabaseMaterializeMySQL::setException(const std::exception_ptr & exception_)
{
    std::unique_lock<std::mutex> lock(mutex);
    exception = exception_;
}

DatabasePtr DatabaseMaterializeMySQL::getNestedDatabase() const
{
    std::unique_lock<std::mutex> lock(mutex);

    if (exception)
    {
        try
        {
            std::rethrow_exception(exception);
        }
        catch (Exception & exception)
        {
            throw Exception(exception);
        }
    }

    return nested_database;
}

ASTPtr DatabaseMaterializeMySQL::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = database_name;
    create_query->set(create_query->storage, engine_define);
    return create_query;
}
void DatabaseMaterializeMySQL::loadStoredObjects(Context & context, bool has_force_restore_data_flag)
{
    try
    {
        LOG_DEBUG(log, "Loading MySQL nested database stored objects.");
        getNestedDatabase()->loadStoredObjects(context, has_force_restore_data_flag);
        LOG_DEBUG(log, "Loaded MySQL nested database stored objects.");
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot load MySQL nested database stored objects.");
        throw;
    }
}

void DatabaseMaterializeMySQL::shutdown()
{
    getNestedDatabase()->shutdown();
}

bool DatabaseMaterializeMySQL::empty() const
{
    return getNestedDatabase()->empty();
}

String DatabaseMaterializeMySQL::getDataPath() const
{
    return getNestedDatabase()->getDataPath();
}

String DatabaseMaterializeMySQL::getMetadataPath() const
{
    return getNestedDatabase()->getMetadataPath();
}

String DatabaseMaterializeMySQL::getTableDataPath(const String & table_name) const
{
    return getNestedDatabase()->getTableDataPath(table_name);
}

String DatabaseMaterializeMySQL::getTableDataPath(const ASTCreateQuery & query) const
{
    return getNestedDatabase()->getTableDataPath(query);
}

String DatabaseMaterializeMySQL::getObjectMetadataPath(const String & table_name) const
{
    return getNestedDatabase()->getObjectMetadataPath(table_name);
}

UUID DatabaseMaterializeMySQL::tryGetTableUUID(const String & table_name) const
{
    return getNestedDatabase()->tryGetTableUUID(table_name);
}

time_t DatabaseMaterializeMySQL::getObjectMetadataModificationTime(const String & name) const
{
    return getNestedDatabase()->getObjectMetadataModificationTime(name);
}

void DatabaseMaterializeMySQL::createTable(const Context & context, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support create table.", ErrorCodes::NOT_IMPLEMENTED);

    getNestedDatabase()->createTable(context, name, table, query);
}

void DatabaseMaterializeMySQL::dropTable(const Context & context, const String & name, bool no_delay)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support drop table.", ErrorCodes::NOT_IMPLEMENTED);

    getNestedDatabase()->dropTable(context, name, no_delay);
}

void DatabaseMaterializeMySQL::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support attach table.", ErrorCodes::NOT_IMPLEMENTED);

    getNestedDatabase()->attachTable(name, table, relative_table_path);
}

StoragePtr DatabaseMaterializeMySQL::detachTable(const String & name)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support detach table.", ErrorCodes::NOT_IMPLEMENTED);

    return getNestedDatabase()->detachTable(name);
}

void DatabaseMaterializeMySQL::renameTable(const Context & context, const String & name, IDatabase & to_database, const String & to_name, bool exchange)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support rename table.", ErrorCodes::NOT_IMPLEMENTED);

    getNestedDatabase()->renameTable(context, name, to_database, to_name, exchange);
}

void DatabaseMaterializeMySQL::alterTable(const Context & context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support alter table.", ErrorCodes::NOT_IMPLEMENTED);

    getNestedDatabase()->alterTable(context, table_id, metadata);
}

bool DatabaseMaterializeMySQL::shouldBeEmptyOnDetach() const
{
    return getNestedDatabase()->shouldBeEmptyOnDetach();
}

void DatabaseMaterializeMySQL::drop(const Context & context)
{
    getNestedDatabase()->drop(context);
}

bool DatabaseMaterializeMySQL::isTableExist(const String & name, const Context & context) const
{
    return getNestedDatabase()->isTableExist(name, context);
}

StoragePtr DatabaseMaterializeMySQL::tryGetTable(const String & name, const Context & context) const
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        return std::make_shared<StorageMaterializeMySQL>(getNestedDatabase()->tryGetTable(name, context));

    return getNestedDatabase()->tryGetTable(name, context);
}

DatabaseTablesIteratorPtr DatabaseMaterializeMySQL::getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
    {
        DatabaseTablesIteratorPtr iterator = getNestedDatabase()->getTablesIterator(context, filter_by_table_name);
        return std::make_unique<DatabaseMaterializeTablesIterator>(std::move(iterator));
    }

    return getNestedDatabase()->getTablesIterator(context, filter_by_table_name);
}

}
