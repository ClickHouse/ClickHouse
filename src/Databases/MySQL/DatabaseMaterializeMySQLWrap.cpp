#include <Databases/MySQL/DatabaseMaterializeMySQLWrap.h>

#include <Common/setThreadName.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/StorageMaterializeMySQL.h>
#include <Databases/MySQL/DatabaseMaterializeTablesIterator.h>

namespace DB
{

static constexpr auto MYSQL_BACKGROUND_THREAD_NAME = "MySQLDBSync";

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

DatabaseMaterializeMySQLWrap::DatabaseMaterializeMySQLWrap(const DatabasePtr & nested_database_, const ASTPtr & database_engine_define_, const String & log_name)
    : IDatabase(nested_database_->getDatabaseName()), nested_database(nested_database_), database_engine_define(database_engine_define_), log(&Logger::get(log_name))
{
}

void DatabaseMaterializeMySQLWrap::setException(const std::exception_ptr & exception_)
{
    std::unique_lock<std::mutex> lock(mutex);
    exception = exception_;
}

DatabasePtr DatabaseMaterializeMySQLWrap::getNestedDatabase() const
{
    std::unique_lock<std::mutex> lock(mutex);

    if (exception)
        std::rethrow_exception(exception);

    return nested_database;
}

ASTPtr DatabaseMaterializeMySQLWrap::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = database_name;
    create_query->set(create_query->storage, database_engine_define);
    return create_query;
}
void DatabaseMaterializeMySQLWrap::loadStoredObjects(Context & context, bool has_force_restore_data_flag)
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

void DatabaseMaterializeMySQLWrap::shutdown()
{
    getNestedDatabase()->shutdown();
}

bool DatabaseMaterializeMySQLWrap::empty() const
{
    return getNestedDatabase()->empty();
}

String DatabaseMaterializeMySQLWrap::getDataPath() const
{
    return getNestedDatabase()->getDataPath();
}

String DatabaseMaterializeMySQLWrap::getMetadataPath() const
{
    return getNestedDatabase()->getMetadataPath();
}

String DatabaseMaterializeMySQLWrap::getTableDataPath(const String & table_name) const
{
    return getNestedDatabase()->getTableDataPath(table_name);
}

String DatabaseMaterializeMySQLWrap::getTableDataPath(const ASTCreateQuery & query) const
{
    return getNestedDatabase()->getTableDataPath(query);
}

String DatabaseMaterializeMySQLWrap::getObjectMetadataPath(const String & table_name) const
{
    return getNestedDatabase()->getObjectMetadataPath(table_name);
}

UUID DatabaseMaterializeMySQLWrap::tryGetTableUUID(const String & table_name) const
{
    return getNestedDatabase()->tryGetTableUUID(table_name);
}

time_t DatabaseMaterializeMySQLWrap::getObjectMetadataModificationTime(const String & name) const
{
    return getNestedDatabase()->getObjectMetadataModificationTime(name);
}

void DatabaseMaterializeMySQLWrap::createTable(const Context & context, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    if (getThreadName() != MYSQL_BACKGROUND_THREAD_NAME)
        throw Exception("MySQL database in locality_data mode does not support create table.", ErrorCodes::NOT_IMPLEMENTED);

    getNestedDatabase()->createTable(context, name, table, query);
}

void DatabaseMaterializeMySQLWrap::dropTable(const Context & context, const String & name, bool no_delay)
{
    if (getThreadName() != MYSQL_BACKGROUND_THREAD_NAME)
        throw Exception("MySQL database in locality_data mode does not support drop table.", ErrorCodes::NOT_IMPLEMENTED);

    getNestedDatabase()->dropTable(context, name, no_delay);
}

void DatabaseMaterializeMySQLWrap::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    if (getThreadName() != MYSQL_BACKGROUND_THREAD_NAME)
        throw Exception("MySQL database in locality_data mode does not support attach table.", ErrorCodes::NOT_IMPLEMENTED);

    getNestedDatabase()->attachTable(name, table, relative_table_path);
}

StoragePtr DatabaseMaterializeMySQLWrap::detachTable(const String & name)
{
    if (getThreadName() != MYSQL_BACKGROUND_THREAD_NAME)
        throw Exception("MySQL database in locality_data mode does not support detach table.", ErrorCodes::NOT_IMPLEMENTED);

    return getNestedDatabase()->detachTable(name);
}

void DatabaseMaterializeMySQLWrap::renameTable(const Context & context, const String & name, IDatabase & to_database, const String & to_name, bool exchange)
{
    if (getThreadName() != MYSQL_BACKGROUND_THREAD_NAME)
        throw Exception("MySQL database in locality_data mode does not support rename table.", ErrorCodes::NOT_IMPLEMENTED);

    getNestedDatabase()->renameTable(context, name, to_database, to_name, exchange);
}

void DatabaseMaterializeMySQLWrap::alterTable(const Context & context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    if (getThreadName() != MYSQL_BACKGROUND_THREAD_NAME)
        throw Exception("MySQL database in locality_data mode does not support alter table.", ErrorCodes::NOT_IMPLEMENTED);

    getNestedDatabase()->alterTable(context, table_id, metadata);
}

bool DatabaseMaterializeMySQLWrap::shouldBeEmptyOnDetach() const
{
    return getNestedDatabase()->shouldBeEmptyOnDetach();
}

void DatabaseMaterializeMySQLWrap::drop(const Context & context)
{
    getNestedDatabase()->drop(context);
}

bool DatabaseMaterializeMySQLWrap::isTableExist(const String & name) const
{
    return getNestedDatabase()->isTableExist(name);
}

StoragePtr DatabaseMaterializeMySQLWrap::tryGetTable(const String & name) const
{
    if (getThreadName() != MYSQL_BACKGROUND_THREAD_NAME)
        return std::make_shared<StorageMaterializeMySQL>(getNestedDatabase()->tryGetTable(name));

    return getNestedDatabase()->tryGetTable(name);
}

DatabaseTablesIteratorPtr DatabaseMaterializeMySQLWrap::getTablesIterator(const FilterByNameFunction & filter_by_table_name)
{
    if (getThreadName() != MYSQL_BACKGROUND_THREAD_NAME)
    {
        DatabaseTablesIteratorPtr iterator = getNestedDatabase()->getTablesIterator(filter_by_table_name);
        return std::make_unique<DatabaseMaterializeTablesIterator>(std::move(iterator));
    }

    return getNestedDatabase()->getTablesIterator(filter_by_table_name);
}

}
