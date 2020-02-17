#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>
#include <IO/WriteHelpers.h>
#include <Storages/StorageID.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseMemory.h>
#include <Poco/File.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int DDL_GUARD_IS_ACTIVE;
    extern const int DATABASE_NOT_EMPTY;
}


void DatabaseCatalog::loadDatabases()
{

    auto db_for_temporary_and_external_tables = std::make_shared<DatabaseMemory>(TEMPORARY_DATABASE);
    attachDatabase(TEMPORARY_DATABASE, db_for_temporary_and_external_tables);
}

void DatabaseCatalog::shutdown()
{
    /** At this point, some tables may have threads that block our mutex.
      * To shutdown them correctly, we will copy the current list of tables,
      *  and ask them all to finish their work.
      * Then delete all objects with tables.
      */

    Databases current_databases;
    {
        std::lock_guard lock(databases_mutex);
        current_databases = databases;
    }

    /// We still hold "databases" (instead of std::move) for Buffer tables to flush data correctly.

    for (auto & database : current_databases)
        database.second->shutdown();


    std::lock_guard lock(databases_mutex);
    for (auto & elem : uuid_map)
    {
        std::lock_guard map_lock(elem.mutex);
        elem.map.clear();
    }
    databases.clear();
    view_dependencies.clear();
}

DatabaseAndTable DatabaseCatalog::tryGetByUUID(const UUID & uuid) const
{
    assert(uuid != UUIDHelpers::Nil && 0 <= getFirstLevelIdx(uuid) && getFirstLevelIdx(uuid) < uuid_map.size());
    const UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto it = map_part.map.find(uuid);
    if (it == map_part.map.end())
        return {};
    return it->second;
}


StoragePtr DatabaseCatalog::getTableImpl(const StorageID & table_id, const Context & local_context, std::optional<Exception> * exception) const
{
    if (!table_id)
    {
        if (exception)
            exception->emplace("Cannot find table: StorageID is empty", ErrorCodes::UNKNOWN_TABLE);
        return {};
    }

    //if (table_id.hasUUID())
    //{
    //    auto db_and_table = tryGetByUUID(table_id.uuid);
    //    if (!db_and_table.first || !db_and_table.second)
    //    {
    //        assert(!db_and_table.first && !db_and_table.second);
    //        if (exception)
    //            exception->emplace("Table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
    //        return {};
//
    //    }
    //    return db_and_table.second;
    //}

    DatabasePtr database;
    {
        std::lock_guard _lock{databases_mutex};
        auto it = databases.find(table_id.getDatabaseName());
        if (databases.end() == it)
        {
            if (exception)
                exception->emplace("Database " + backQuoteIfNeed(table_id.getDatabaseName()) + " doesn't exist",
                                   ErrorCodes::UNKNOWN_DATABASE);
            return {};
        }
        database = it->second;
    }

    auto table = database->tryGetTable(local_context, table_id.table_name);
    if (!table && exception)
            exception->emplace("Table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    return table;
}

void DatabaseCatalog::assertDatabaseExists(const String & database_name) const
{
    std::lock_guard lock{databases_mutex};
    assertDatabaseExistsUnlocked(database_name);
}

void DatabaseCatalog::assertDatabaseDoesntExist(const String & database_name) const
{
    std::lock_guard lock{databases_mutex};
    assertDatabaseDoesntExistUnlocked(database_name);
}

void DatabaseCatalog::assertDatabaseExistsUnlocked(const String & database_name) const
{
    assert(!database_name.empty());
    if (databases.end() == databases.find(database_name))
        throw Exception("Database " + backQuoteIfNeed(database_name) + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
}


void DatabaseCatalog::assertDatabaseDoesntExistUnlocked(const String & database_name) const
{
    assert(!database_name.empty());
    if (databases.end() != databases.find(database_name))
        throw Exception("Database " + backQuoteIfNeed(database_name) + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
}

void DatabaseCatalog::attachDatabase(const String & database_name, const DatabasePtr & database)
{
    std::lock_guard lock{databases_mutex};
    assertDatabaseDoesntExistUnlocked(database_name);
    databases[database_name] = database;
}


DatabasePtr DatabaseCatalog::detachDatabase(const String & database_name, bool drop)
{
    std::lock_guard lock{databases_mutex};
    assertDatabaseExistsUnlocked(database_name);
    auto db = databases.find(database_name)->second;

    if (!db->empty(*global_context))
    if (!db->empty(*global_context))
        throw Exception("New table appeared in database being dropped or detached. Try again.", ErrorCodes::DATABASE_NOT_EMPTY);

    databases.erase(database_name);

    db->shutdown();

    if (drop)
    {
        /// Delete the database.
        db->drop(*global_context);

        /// Old ClickHouse versions did not store database.sql files
        Poco::File database_metadata_file(
                global_context->getPath() + "metadata/" + escapeForFileName(database_name) + ".sql");
        if (database_metadata_file.exists())
            database_metadata_file.remove(false);
    }

    return db;
}

DatabasePtr DatabaseCatalog::getDatabase(const String & database_name) const
{
    std::lock_guard lock{databases_mutex};
    assertDatabaseExistsUnlocked(database_name);
    return databases.find(database_name)->second;
}

DatabasePtr DatabaseCatalog::tryGetDatabase(const String & database_name) const
{
    assert(!database_name.empty());
    std::lock_guard lock{databases_mutex};
    auto it = databases.find(database_name);
    if (it == databases.end())
        return {};
    return it->second;
}

bool DatabaseCatalog::isDatabaseExist(const String & database_name) const
{
    assert(!database_name.empty());
    std::lock_guard lock{databases_mutex};
    return databases.end() != databases.find(database_name);
}

Databases DatabaseCatalog::getDatabases() const
{
    std::lock_guard lock{databases_mutex};
    return databases;
}

bool DatabaseCatalog::isTableExist(const DB::StorageID & table_id, const DB::Context & context) const
{
    //if (table_id.hasUUID())
    //    return tryGetByUUID(table_id.uuid).second != nullptr;
    //else
    //{
        DatabasePtr db;
        {
            std::lock_guard lock{databases_mutex};
            auto iter = databases.find(table_id.database_name);
            if (iter != databases.end())
                db = iter->second;
        }
        return db && db->isTableExist(context, table_id.table_name);
    //}
}

void DatabaseCatalog::assertTableDoesntExist(const StorageID & table_id, const Context & context) const
{
    if (isTableExist(table_id, context))
        throw Exception("Table " + table_id.getNameForLogs() + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);

}

DatabasePtr DatabaseCatalog::getDatabaseForTemporaryTables() const
{
    return getDatabase(TEMPORARY_DATABASE);
}

DatabasePtr DatabaseCatalog::getSystemDatabase() const
{
    return getDatabase(SYSTEM_DATABASE);
}

void DatabaseCatalog::addUUIDMapping(const UUID & uuid, DatabasePtr database, StoragePtr table)
{
    assert(uuid != UUIDHelpers::Nil && 0 <= getFirstLevelIdx(uuid) && getFirstLevelIdx(uuid) < uuid_map.size());
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto [_, inserted] = map_part.map.try_emplace(uuid, std::move(database), std::move(table));
    if (!inserted)
        throw Exception("Mapping for table with UUID=" + toString(uuid) + " already exists", ErrorCodes::LOGICAL_ERROR);
}

void DatabaseCatalog::removeUUIDMapping(const UUID & uuid)
{
    assert(uuid != UUIDHelpers::Nil && 0 <= getFirstLevelIdx(uuid) && getFirstLevelIdx(uuid) < uuid_map.size());
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    if (!map_part.map.erase(uuid))
        throw Exception("Mapping for table with UUID=" + toString(uuid) + " doesn't exist", ErrorCodes::LOGICAL_ERROR);
}

DatabaseCatalog::DatabaseCatalog(const Context * global_context_)
    : global_context(global_context_), log(&Poco::Logger::get("DatabaseCatalog"))
{
    if (!global_context)
        throw Exception("DatabaseCatalog is not initialized. It's a bug.", ErrorCodes::LOGICAL_ERROR);
}

DatabaseCatalog & DatabaseCatalog::init(const Context * global_context_)
{
    static DatabaseCatalog database_catalog(global_context_);
    return database_catalog;
}

DatabaseCatalog & DatabaseCatalog::instance()
{
    return init(nullptr);
}

DatabasePtr DatabaseCatalog::getDatabase(const String & database_name, const Context & local_context) const
{
    String resolved_database = local_context.resolveDatabase(database_name);
    return getDatabase(resolved_database);
}


void DatabaseCatalog::addDependency(const StorageID & from, const StorageID & where)
{
    std::lock_guard lock{databases_mutex};
    // FIXME when loading metadata storage may not know UUIDs of it's dependencies, because they are not loaded yet,
    // so UUID of `from` is not used here. (same for remove, get and update)
    view_dependencies[{from.getDatabaseName(), from.getTableName()}].insert(where);

}

void DatabaseCatalog::removeDependency(const StorageID & from, const StorageID & where)
{
    std::lock_guard lock{databases_mutex};
    view_dependencies[{from.getDatabaseName(), from.getTableName()}].erase(where);
}

Dependencies DatabaseCatalog::getDependencies(const StorageID & from) const
{
    std::lock_guard lock{databases_mutex};
    auto iter = view_dependencies.find({from.getDatabaseName(), from.getTableName()});
    if (iter == view_dependencies.end())
        return {};
    return Dependencies(iter->second.begin(), iter->second.end());
}

void
DatabaseCatalog::updateDependency(const StorageID & old_from, const StorageID & old_where, const StorageID & new_from,
                                  const StorageID & new_where)
{
    std::lock_guard lock{databases_mutex};
    if (!old_from.empty())
        view_dependencies[{old_from.getDatabaseName(), old_from.getTableName()}].erase(old_where);
    if (!new_from.empty())
        view_dependencies[{new_from.getDatabaseName(), new_from.getTableName()}].insert(new_where);
}

std::unique_ptr<DDLGuard> DatabaseCatalog::getDDLGuard(const String & database, const String & table)
{
    std::unique_lock lock(ddl_guards_mutex);
    return std::make_unique<DDLGuard>(ddl_guards[database], std::move(lock), table);
}

bool DatabaseCatalog::isDictionaryExist(const StorageID & table_id, const Context & context) const
{
    auto db = tryGetDatabase(table_id.getDatabaseName());
    return db && db->isDictionaryExist(context, table_id.getTableName());
}

StoragePtr DatabaseCatalog::getTable(const StorageID & table_id) const
{
    std::optional<Exception> exc;
    auto res = getTableImpl(table_id, *global_context, &exc);
    if (!res)
        throw *exc;
    return res;
}

StoragePtr DatabaseCatalog::tryGetTable(const StorageID & table_id) const
{
    return getTableImpl(table_id, *global_context, nullptr);
}


DDLGuard::DDLGuard(Map & map_, std::unique_lock<std::mutex> guards_lock_, const String & elem)
        : map(map_), guards_lock(std::move(guards_lock_))
{
    it = map.emplace(elem, Entry{std::make_unique<std::mutex>(), 0}).first;
    ++it->second.counter;
    guards_lock.unlock();
    table_lock = std::unique_lock(*it->second.mutex);
}

DDLGuard::~DDLGuard()
{
    guards_lock.lock();
    --it->second.counter;
    if (!it->second.counter)
    {
        table_lock.unlock();
        map.erase(it);
    }
}

}


