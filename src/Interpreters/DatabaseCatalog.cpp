#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>
#include <Storages/IStorage.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseAtomic.h>
#include <Poco/File.h>
#include <Common/quoteString.h>
#include <Storages/StorageMemory.h>
#include <Core/BackgroundSchedulePool.h>
#include <Parsers/formatAST.h>
#include <IO/ReadHelpers.h>
#include <Poco/DirectoryIterator.h>

#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int DATABASE_NOT_EMPTY;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
}

TemporaryTableHolder::TemporaryTableHolder(const Context & context_,
                                           const TemporaryTableHolder::Creator & creator, const ASTPtr & query)
    : global_context(&context_.getGlobalContext())
    , temporary_tables(DatabaseCatalog::instance().getDatabaseForTemporaryTables().get())
{
    ASTPtr original_create;
    ASTCreateQuery * create = dynamic_cast<ASTCreateQuery *>(query.get());
    String global_name;
    if (query)
    {
        original_create = create->clone();
        if (create->uuid == UUIDHelpers::Nil)
            create->uuid = UUIDHelpers::generateV4();
        id = create->uuid;
        create->table = "_tmp_" + toString(id);
        global_name = create->table;
        create->database = DatabaseCatalog::TEMPORARY_DATABASE;
    }
    else
    {
        id = UUIDHelpers::generateV4();
        global_name = "_tmp_" + toString(id);
    }
    auto table_id = StorageID(DatabaseCatalog::TEMPORARY_DATABASE, global_name, id);
    auto table = creator(table_id);
    temporary_tables->createTable(*global_context, global_name, table, original_create);
    table->startup();
}


TemporaryTableHolder::TemporaryTableHolder(
    const Context & context_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    const ASTPtr & query)
    : TemporaryTableHolder
      (
          context_,
          [&](const StorageID & table_id)
          {
              return StorageMemory::create(table_id, ColumnsDescription{columns}, ConstraintsDescription{constraints});
          },
          query
      )
{
}

TemporaryTableHolder::TemporaryTableHolder(TemporaryTableHolder && rhs)
        : global_context(rhs.global_context), temporary_tables(rhs.temporary_tables), id(rhs.id)
{
    rhs.id = UUIDHelpers::Nil;
}

TemporaryTableHolder & TemporaryTableHolder::operator = (TemporaryTableHolder && rhs)
{
    id = rhs.id;
    rhs.id = UUIDHelpers::Nil;
    return *this;
}

TemporaryTableHolder::~TemporaryTableHolder()
{
    if (id != UUIDHelpers::Nil)
        temporary_tables->dropTable(*global_context, "_tmp_" + toString(id));
}

StorageID TemporaryTableHolder::getGlobalTableID() const
{
    return StorageID{DatabaseCatalog::TEMPORARY_DATABASE, "_tmp_" + toString(id), id};
}

StoragePtr TemporaryTableHolder::getTable() const
{
    auto table = temporary_tables->tryGetTable("_tmp_" + toString(id), *global_context);
    if (!table)
        throw Exception("Temporary table " + getGlobalTableID().getNameForLogs() + " not found", ErrorCodes::LOGICAL_ERROR);
    return table;
}


void DatabaseCatalog::loadDatabases()
{
    drop_delay_sec = global_context->getConfigRef().getInt("database_atomic_delay_before_drop_table_sec", default_drop_delay_sec);

    auto db_for_temporary_and_external_tables = std::make_shared<DatabaseMemory>(TEMPORARY_DATABASE, *global_context);
    attachDatabase(TEMPORARY_DATABASE, db_for_temporary_and_external_tables);

    loadMarkedAsDroppedTables();
    auto task_holder = global_context->getSchedulePool().createTask("DatabaseCatalog", [this](){ this->dropTableDataTask(); });
    drop_task = std::make_unique<BackgroundSchedulePoolTaskHolder>(std::move(task_holder));
    (*drop_task)->activateAndSchedule();
}

void DatabaseCatalog::shutdownImpl()
{
    if (drop_task)
        (*drop_task)->deactivate();

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
    assert(std::find_if_not(uuid_map.begin(), uuid_map.end(), [](const auto & elem) { return elem.map.empty(); }) == uuid_map.end());
    databases.clear();
    view_dependencies.clear();
}

DatabaseAndTable DatabaseCatalog::tryGetByUUID(const UUID & uuid) const
{
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    const UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto it = map_part.map.find(uuid);
    if (it == map_part.map.end())
        return {};
    return it->second;
}


DatabaseAndTable DatabaseCatalog::getTableImpl(
    const StorageID & table_id,
    const Context & context,
    std::optional<Exception> * exception) const
{
    if (!table_id)
    {
        if (exception)
            exception->emplace("Cannot find table: StorageID is empty", ErrorCodes::UNKNOWN_TABLE);
        return {};
    }

    if (table_id.hasUUID())
    {
        /// Shortcut for tables which have persistent UUID
        auto db_and_table = tryGetByUUID(table_id.uuid);
        if (!db_and_table.first || !db_and_table.second)
        {
            assert(!db_and_table.first && !db_and_table.second);
            if (exception)
                exception->emplace("Table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
            return {};
        }
        return db_and_table;
    }

    if (table_id.database_name == TEMPORARY_DATABASE)
    {
        /// For temporary tables UUIDs are set in Context::resolveStorageID(...).
        /// If table_id has no UUID, then the name of database was specified by user and table_id was not resolved through context.
        /// Do not allow access to TEMPORARY_DATABASE because it contains all temporary tables of all contexts and users.
        if (exception)
            exception->emplace("Direct access to `" + String(TEMPORARY_DATABASE) + "` database is not allowed.", ErrorCodes::DATABASE_ACCESS_DENIED);
        return {};
    }

    DatabasePtr database;
    {
        std::lock_guard lock{databases_mutex};
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

    auto table = database->tryGetTable(table_id.table_name, context);
    if (!table && exception)
            exception->emplace("Table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    return {database, table};
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


DatabasePtr DatabaseCatalog::detachDatabase(const String & database_name, bool drop, bool check_empty)
{
    if (database_name == TEMPORARY_DATABASE)
        throw Exception("Cannot detach database with temporary tables.", ErrorCodes::DATABASE_ACCESS_DENIED);

    std::shared_ptr<IDatabase> db;
    {
        std::lock_guard lock{databases_mutex};
        assertDatabaseExistsUnlocked(database_name);
        db = databases.find(database_name)->second;

        if (check_empty)
        {
            if (!db->empty())
                throw Exception("New table appeared in database being dropped or detached. Try again.",
                                ErrorCodes::DATABASE_NOT_EMPTY);
            auto * database_atomic = typeid_cast<DatabaseAtomic *>(db.get());
            if (!drop && database_atomic)
                database_atomic->assertCanBeDetached(false);
        }

        databases.erase(database_name);
    }

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

bool DatabaseCatalog::isTableExist(const DB::StorageID & table_id, const Context & context) const
{
    if (table_id.hasUUID())
        return tryGetByUUID(table_id.uuid).second != nullptr;

    DatabasePtr db;
    {
        std::lock_guard lock{databases_mutex};
        auto iter = databases.find(table_id.database_name);
        if (iter != databases.end())
            db = iter->second;
    }
    return db && db->isTableExist(table_id.table_name, context);
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
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto [_, inserted] = map_part.map.try_emplace(uuid, std::move(database), std::move(table));
    if (!inserted)
        throw Exception("Mapping for table with UUID=" + toString(uuid) + " already exists", ErrorCodes::LOGICAL_ERROR);
}

void DatabaseCatalog::removeUUIDMapping(const UUID & uuid)
{
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    if (!map_part.map.erase(uuid))
        throw Exception("Mapping for table with UUID=" + toString(uuid) + " doesn't exist", ErrorCodes::LOGICAL_ERROR);
}

void DatabaseCatalog::updateUUIDMapping(const UUID & uuid, DatabasePtr database, StoragePtr table)
{
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto it = map_part.map.find(uuid);
    if (it == map_part.map.end())
        throw Exception("Mapping for table with UUID=" + toString(uuid) + " doesn't exist", ErrorCodes::LOGICAL_ERROR);
    it->second = std::make_pair(std::move(database), std::move(table));
}

std::unique_ptr<DatabaseCatalog> DatabaseCatalog::database_catalog;

DatabaseCatalog::DatabaseCatalog(Context * global_context_)
    : global_context(global_context_), log(&Poco::Logger::get("DatabaseCatalog"))
{
    if (!global_context)
        throw Exception("DatabaseCatalog is not initialized. It's a bug.", ErrorCodes::LOGICAL_ERROR);
}

DatabaseCatalog & DatabaseCatalog::init(Context * global_context_)
{
    if (database_catalog)
    {
        throw Exception("Database catalog is initialized twice. This is a bug.",
            ErrorCodes::LOGICAL_ERROR);
    }

    database_catalog.reset(new DatabaseCatalog(global_context_));

    return *database_catalog;
}

DatabaseCatalog & DatabaseCatalog::instance()
{
    if (!database_catalog)
    {
        throw Exception("Database catalog is not initialized. This is a bug.",
            ErrorCodes::LOGICAL_ERROR);
    }

    return *database_catalog;
}

void DatabaseCatalog::shutdown()
{
    // The catalog might not be initialized yet by init(global_context). It can
    // happen if some exception was thrown on first steps of startup.
    if (database_catalog)
    {
        database_catalog->shutdownImpl();
    }
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

bool DatabaseCatalog::isDictionaryExist(const StorageID & table_id) const
{
    auto db = tryGetDatabase(table_id.getDatabaseName());
    return db && db->isDictionaryExist(table_id.getTableName());
}

StoragePtr DatabaseCatalog::getTable(const StorageID & table_id, const Context & context) const
{
    std::optional<Exception> exc;
    auto res = getTableImpl(table_id, context, &exc);
    if (!res.second)
        throw Exception(*exc);
    return res.second;
}

StoragePtr DatabaseCatalog::tryGetTable(const StorageID & table_id, const Context & context) const
{
    return getTableImpl(table_id, context, nullptr).second;
}

DatabaseAndTable DatabaseCatalog::getDatabaseAndTable(const StorageID & table_id, const Context & context) const
{
    std::optional<Exception> exc;
    auto res = getTableImpl(table_id, context, &exc);
    if (!res.second)
        throw Exception(*exc);
    return res;
}

DatabaseAndTable DatabaseCatalog::tryGetDatabaseAndTable(const StorageID & table_id, const Context & context) const
{
    return getTableImpl(table_id, context, nullptr);
}

void DatabaseCatalog::loadMarkedAsDroppedTables()
{
    /// /clickhouse_root/metadata_dropped/ contains files with metadata of tables,
    /// which where marked as dropped by Atomic databases.
    /// Data directories of such tables still exists in store/
    /// and metadata still exists in ZooKeeper for ReplicatedMergeTree tables.
    /// If server restarts before such tables was completely dropped,
    /// we should load them and enqueue cleanup to remove data from store/ and metadata from ZooKeeper

    std::map<String, StorageID> dropped_metadata;
    String path = global_context->getPath() + "metadata_dropped/";

    if (!std::filesystem::exists(path))
    {
        return;
    }

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(path); it != dir_end; ++it)
    {
        /// File name has the following format:
        /// database_name.table_name.uuid.sql

        /// Ignore unexpected files
        if (!it.name().ends_with(".sql"))
            continue;

        /// Process .sql files with metadata of tables which were marked as dropped
        StorageID dropped_id = StorageID::createEmpty();
        size_t dot_pos = it.name().find('.');
        if (dot_pos == std::string::npos)
            continue;
        dropped_id.database_name = unescapeForFileName(it.name().substr(0, dot_pos));

        size_t prev_dot_pos = dot_pos;
        dot_pos = it.name().find('.', prev_dot_pos + 1);
        if (dot_pos == std::string::npos)
            continue;
        dropped_id.table_name = unescapeForFileName(it.name().substr(prev_dot_pos + 1, dot_pos - prev_dot_pos - 1));

        prev_dot_pos = dot_pos;
        dot_pos = it.name().find('.', prev_dot_pos + 1);
        if (dot_pos == std::string::npos)
            continue;
        dropped_id.uuid = parse<UUID>(it.name().substr(prev_dot_pos + 1, dot_pos - prev_dot_pos - 1));

        String full_path = path + it.name();
        dropped_metadata.emplace(std::move(full_path), std::move(dropped_id));
    }

    ThreadPool pool;
    for (const auto & elem : dropped_metadata)
    {
        pool.scheduleOrThrowOnError([&]()
        {
            this->enqueueDroppedTableCleanup(elem.second, nullptr, elem.first);
        });
    }
    pool.wait();
}

String DatabaseCatalog::getPathForDroppedMetadata(const StorageID & table_id) const
{
    return global_context->getPath() + "metadata_dropped/" +
           escapeForFileName(table_id.getDatabaseName()) + "." +
           escapeForFileName(table_id.getTableName()) + "." +
           toString(table_id.uuid) + ".sql";
}

void DatabaseCatalog::enqueueDroppedTableCleanup(StorageID table_id, StoragePtr table, String dropped_metadata_path, bool ignore_delay)
{
    assert(table_id.hasUUID());
    assert(!table || table->getStorageID().uuid == table_id.uuid);
    assert(dropped_metadata_path == getPathForDroppedMetadata(table_id));

    /// Table was removed from database. Enqueue removal of its data from disk.
    time_t drop_time;
    if (table)
        drop_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    else
    {
        /// Try load table from metadata to drop it correctly (e.g. remove metadata from zk or remove data from all volumes)
        LOG_INFO(log, "Trying load partially dropped table {} from {}", table_id.getNameForLogs(), dropped_metadata_path);
        ASTPtr ast = DatabaseOnDisk::parseQueryFromMetadata(log, *global_context, dropped_metadata_path, /*throw_on_error*/ false, /*remove_empty*/false);
        auto * create = typeid_cast<ASTCreateQuery *>(ast.get());
        assert(!create || create->uuid == table_id.uuid);

        if (create)
        {
            String data_path = "store/" + getPathForUUID(table_id.uuid);
            create->database = table_id.database_name;
            create->table = table_id.table_name;
            try
            {
                table = createTableFromAST(*create, table_id.getDatabaseName(), data_path, *global_context, false).second;
            }
            catch (...)
            {
                tryLogCurrentException(log, "Cannot load partially dropped table " + table_id.getNameForLogs() +
                                            " from: " + dropped_metadata_path +
                                            ". Parsed query: " + serializeAST(*create) +
                                            ". Will remove metadata and " + data_path +
                                            ". Garbage may be left in ZooKeeper.");
            }
        }
        else
        {
            LOG_WARNING(log, "Cannot parse metadata of partially dropped table {} from {}. Will remove metadata file and data directory. Garbage may be left in /store directory and ZooKeeper.", table_id.getNameForLogs(), dropped_metadata_path);
        }

        drop_time = Poco::File(dropped_metadata_path).getLastModified().epochTime();
    }

    std::lock_guard lock(tables_marked_dropped_mutex);
    if (ignore_delay)
        tables_marked_dropped.push_front({table_id, table, dropped_metadata_path, 0});
    else
        tables_marked_dropped.push_back({table_id, table, dropped_metadata_path, drop_time});
    /// If list of dropped tables was empty, start a drop task
    if (drop_task && tables_marked_dropped.size() == 1)
        (*drop_task)->schedule();
}

void DatabaseCatalog::dropTableDataTask()
{
    /// Background task that removes data of tables which were marked as dropped by Atomic databases.
    /// Table can be removed when it's not used by queries and drop_delay_sec elapsed since it was marked as dropped.

    bool need_reschedule = true;
    TableMarkedAsDropped table;
    try
    {
        std::lock_guard lock(tables_marked_dropped_mutex);
        time_t current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        auto it = std::find_if(tables_marked_dropped.begin(), tables_marked_dropped.end(), [&](const auto & elem)
        {
            bool not_in_use = !elem.table || elem.table.unique();
            bool old_enough = elem.drop_time + drop_delay_sec < current_time;
            return not_in_use && old_enough;
        });
        if (it != tables_marked_dropped.end())
        {
            table = std::move(*it);
            LOG_INFO(log, "Will try drop {}", table.table_id.getNameForLogs());
            tables_marked_dropped.erase(it);
        }
        need_reschedule = !tables_marked_dropped.empty();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    if (table.table_id)
    {

        try
        {
            dropTableFinally(table);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot drop table " + table.table_id.getNameForLogs() +
                                        ". Will retry later.");
            {
                std::lock_guard lock(tables_marked_dropped_mutex);
                tables_marked_dropped.emplace_back(std::move(table));
                /// If list of dropped tables was empty, schedule a task to retry deletion.
                if (tables_marked_dropped.size() == 1)
                    need_reschedule = true;
            }
        }
    }

    /// Do not schedule a task if there is no tables to drop
    if (need_reschedule)
        (*drop_task)->scheduleAfter(reschedule_time_ms);
}

void DatabaseCatalog::dropTableFinally(const TableMarkedAsDropped & table) const
{
    if (table.table)
    {
        table.table->drop();
        table.table->is_dropped = true;
    }

    /// Even if table is not loaded, try remove its data from disk.
    /// TODO remove data from all volumes
    String data_path = global_context->getPath() + "store/" + getPathForUUID(table.table_id.uuid);
    Poco::File table_data_dir{data_path};
    if (table_data_dir.exists())
    {
        LOG_INFO(log, "Removing data directory {} of dropped table {}", data_path, table.table_id.getNameForLogs());
        table_data_dir.remove(true);
    }

    LOG_INFO(log, "Removing metadata {} of dropped table {}", table.metadata_path, table.table_id.getNameForLogs());
    Poco::File(table.metadata_path).remove();
}

String DatabaseCatalog::getPathForUUID(const UUID & uuid)
{
    const size_t uuid_prefix_len = 3;
    return toString(uuid).substr(0, uuid_prefix_len) + '/' + toString(uuid) + '/';
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
