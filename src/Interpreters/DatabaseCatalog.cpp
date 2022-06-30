#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>
#include <Storages/IStorage.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOnDisk.h>
#include <Common/quoteString.h>
#include <Storages/StorageMemory.h>
#include <Storages/LiveView/TemporaryLiveViewCleaner.h>
#include <Core/BackgroundSchedulePool.h>
#include <Parsers/formatAST.h>
#include <IO/ReadHelpers.h>
#include <Poco/DirectoryIterator.h>
#include <Common/atomicRename.h>
#include <Common/CurrentMetrics.h>
#include <Common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>
#include <Common/filesystemHelpers.h>

#include "config_core.h"

#if USE_MYSQL
#    include <Databases/MySQL/MaterializedMySQLSyncThread.h>
#    include <Storages/StorageMaterializedMySQL.h>
#endif

#if USE_LIBPQXX
#    include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#    include <Databases/PostgreSQL/DatabaseMaterializedPostgreSQL.h>
#endif

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric TablesToDropQueueSize;
}

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
    extern const int HAVE_DEPENDENT_OBJECTS;
}

TemporaryTableHolder::TemporaryTableHolder(ContextPtr context_, const TemporaryTableHolder::Creator & creator, const ASTPtr & query)
    : WithContext(context_->getGlobalContext())
    , temporary_tables(DatabaseCatalog::instance().getDatabaseForTemporaryTables().get())
{
    ASTPtr original_create;
    ASTCreateQuery * create = dynamic_cast<ASTCreateQuery *>(query.get());
    String global_name;
    if (create)
    {
        original_create = create->clone();
        if (create->uuid == UUIDHelpers::Nil)
            create->uuid = UUIDHelpers::generateV4();
        id = create->uuid;
        create->setTable("_tmp_" + toString(id));
        global_name = create->getTable();
        create->setDatabase(DatabaseCatalog::TEMPORARY_DATABASE);
    }
    else
    {
        id = UUIDHelpers::generateV4();
        global_name = "_tmp_" + toString(id);
    }
    auto table_id = StorageID(DatabaseCatalog::TEMPORARY_DATABASE, global_name, id);
    auto table = creator(table_id);
    temporary_tables->createTable(getContext(), global_name, table, original_create);
    table->startup();
}


TemporaryTableHolder::TemporaryTableHolder(
    ContextPtr context_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    const ASTPtr & query,
    bool create_for_global_subquery)
    : TemporaryTableHolder(
        context_,
        [&](const StorageID & table_id)
        {
            auto storage = std::make_shared<StorageMemory>(table_id, ColumnsDescription{columns}, ConstraintsDescription{constraints}, String{});

            if (create_for_global_subquery)
                storage->delayReadForGlobalSubqueries();

            return storage;
        },
        query)
{
}

TemporaryTableHolder::TemporaryTableHolder(TemporaryTableHolder && rhs) noexcept
        : WithContext(rhs.context), temporary_tables(rhs.temporary_tables), id(rhs.id)
{
    rhs.id = UUIDHelpers::Nil;
}

TemporaryTableHolder & TemporaryTableHolder::operator=(TemporaryTableHolder && rhs) noexcept
{
    id = rhs.id;
    rhs.id = UUIDHelpers::Nil;
    return *this;
}

TemporaryTableHolder::~TemporaryTableHolder()
{
    if (id != UUIDHelpers::Nil)
    {
        auto table = getTable();
        table->flushAndShutdown();
        temporary_tables->dropTable(getContext(), "_tmp_" + toString(id));
    }
}

StorageID TemporaryTableHolder::getGlobalTableID() const
{
    return StorageID{DatabaseCatalog::TEMPORARY_DATABASE, "_tmp_" + toString(id), id};
}

StoragePtr TemporaryTableHolder::getTable() const
{
    auto table = temporary_tables->tryGetTable("_tmp_" + toString(id), getContext());
    if (!table)
        throw Exception("Temporary table " + getGlobalTableID().getNameForLogs() + " not found", ErrorCodes::LOGICAL_ERROR);
    return table;
}


void DatabaseCatalog::initializeAndLoadTemporaryDatabase()
{
    drop_delay_sec = getContext()->getConfigRef().getInt("database_atomic_delay_before_drop_table_sec", default_drop_delay_sec);

    auto db_for_temporary_and_external_tables = std::make_shared<DatabaseMemory>(TEMPORARY_DATABASE, getContext());
    attachDatabase(TEMPORARY_DATABASE, db_for_temporary_and_external_tables);
}

void DatabaseCatalog::loadDatabases()
{
    auto task_holder = getContext()->getSchedulePool().createTask("DatabaseCatalog", [this](){ this->dropTableDataTask(); });
    drop_task = std::make_unique<BackgroundSchedulePoolTaskHolder>(std::move(task_holder));
    (*drop_task)->activate();
    std::lock_guard lock{tables_marked_dropped_mutex};
    if (!tables_marked_dropped.empty())
        (*drop_task)->schedule();

    /// Another background thread which drops temporary LiveViews.
    /// We should start it after loadMarkedAsDroppedTables() to avoid race condition.
    TemporaryLiveViewCleaner::instance().startup();
}

void DatabaseCatalog::shutdownImpl()
{
    TemporaryLiveViewCleaner::shutdown();

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

    tables_marked_dropped.clear();

    std::lock_guard lock(databases_mutex);
    assert(std::find_if(uuid_map.begin(), uuid_map.end(), [](const auto & elem)
    {
        /// Ensure that all UUID mappings are empty (i.e. all mappings contain nullptr instead of a pointer to storage)
        const auto & not_empty_mapping = [] (const auto & mapping)
        {
            auto & table = mapping.second.second;
            return table;
        };
        auto it = std::find_if(elem.map.begin(), elem.map.end(), not_empty_mapping);
        return it != elem.map.end();
    }) == uuid_map.end());
    databases.clear();
    db_uuid_map.clear();
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
    ContextPtr context_,
    std::optional<Exception> * exception) const
{
    if (!table_id)
    {
        if (exception)
            exception->emplace(ErrorCodes::UNKNOWN_TABLE, "Cannot find table: StorageID is empty");
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
                exception->emplace(fmt::format("Table {} doesn't exist", table_id.getNameForLogs()), ErrorCodes::UNKNOWN_TABLE);
            return {};
        }

#if USE_LIBPQXX
        if (!context_->isInternalQuery() && (db_and_table.first->getEngineName() == "MaterializedPostgreSQL"))
        {
            db_and_table.second = std::make_shared<StorageMaterializedPostgreSQL>(std::move(db_and_table.second), getContext(),
                                        assert_cast<const DatabaseMaterializedPostgreSQL *>(db_and_table.first.get())->getPostgreSQLDatabaseName(),
                                        db_and_table.second->getStorageID().table_name);
        }
#endif

#if USE_MYSQL
        /// It's definitely not the best place for this logic, but behaviour must be consistent with DatabaseMaterializedMySQL::tryGetTable(...)
        if (!context_->isInternalQuery() && db_and_table.first->getEngineName() == "MaterializedMySQL")
        {
            db_and_table.second = std::make_shared<StorageMaterializedMySQL>(std::move(db_and_table.second), db_and_table.first.get());
        }
#endif
        return db_and_table;
    }


    if (table_id.database_name == TEMPORARY_DATABASE)
    {
        /// For temporary tables UUIDs are set in Context::resolveStorageID(...).
        /// If table_id has no UUID, then the name of database was specified by user and table_id was not resolved through context.
        /// Do not allow access to TEMPORARY_DATABASE because it contains all temporary tables of all contexts and users.
        if (exception)
            exception->emplace(fmt::format("Direct access to `{}` database is not allowed", TEMPORARY_DATABASE), ErrorCodes::DATABASE_ACCESS_DENIED);
        return {};
    }

    DatabasePtr database;
    {
        std::lock_guard lock{databases_mutex};
        auto it = databases.find(table_id.getDatabaseName());
        if (databases.end() == it)
        {
            if (exception)
                exception->emplace(fmt::format("Database {} doesn't exist", backQuoteIfNeed(table_id.getDatabaseName())), ErrorCodes::UNKNOWN_DATABASE);
            return {};
        }
        database = it->second;
    }

    auto table = database->tryGetTable(table_id.table_name, context_);
    if (!table && exception)
            exception->emplace(fmt::format("Table {} doesn't exist", table_id.getNameForLogs()), ErrorCodes::UNKNOWN_TABLE);
    if (!table)
        database = nullptr;

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
    databases.emplace(database_name, database);
    UUID db_uuid = database->getUUID();
    if (db_uuid != UUIDHelpers::Nil)
        db_uuid_map.emplace(db_uuid, database);
}


DatabasePtr DatabaseCatalog::detachDatabase(ContextPtr local_context, const String & database_name, bool drop, bool check_empty)
{
    if (database_name == TEMPORARY_DATABASE)
        throw Exception("Cannot detach database with temporary tables.", ErrorCodes::DATABASE_ACCESS_DENIED);

    DatabasePtr db;
    {
        std::lock_guard lock{databases_mutex};
        assertDatabaseExistsUnlocked(database_name);
        db = databases.find(database_name)->second;
        db_uuid_map.erase(db->getUUID());
        databases.erase(database_name);
    }

    if (check_empty)
    {
        try
        {
            if (!db->empty())
                throw Exception("New table appeared in database being dropped or detached. Try again.",
                                ErrorCodes::DATABASE_NOT_EMPTY);
            if (!drop)
                db->assertCanBeDetached(false);
        }
        catch (...)
        {
            attachDatabase(database_name, db);
            throw;
        }
    }

    db->shutdown();

    if (drop)
    {
        /// Delete the database.
        db->drop(local_context);

        /// Old ClickHouse versions did not store database.sql files
        /// Remove metadata dir (if exists) to avoid recreation of .sql file on server startup
        fs::path database_metadata_dir = fs::path(getContext()->getPath()) / "metadata" / escapeForFileName(database_name);
        fs::remove(database_metadata_dir);
        fs::path database_metadata_file = fs::path(getContext()->getPath()) / "metadata" / (escapeForFileName(database_name) + ".sql");
        fs::remove(database_metadata_file);
    }

    return db;
}

void DatabaseCatalog::updateDatabaseName(const String & old_name, const String & new_name, const Strings & tables_in_database)
{
    std::lock_guard lock{databases_mutex};
    assert(databases.find(new_name) == databases.end());
    auto it = databases.find(old_name);
    assert(it != databases.end());
    auto db = it->second;
    databases.erase(it);
    databases.emplace(new_name, db);

    for (const auto & table_name : tables_in_database)
    {
        QualifiedTableName new_table_name{new_name, table_name};
        auto dependencies = tryRemoveLoadingDependenciesUnlocked(QualifiedTableName{old_name, table_name}, /* check_dependencies */ false);
        DependenciesInfos new_info;
        for (const auto & dependency : dependencies)
            new_info[dependency].dependent_database_objects.insert(new_table_name);
        new_info[new_table_name].dependencies = std::move(dependencies);
        mergeDependenciesGraphs(loading_dependencies, new_info);
    }
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

DatabasePtr DatabaseCatalog::getDatabase(const UUID & uuid) const
{
    std::lock_guard lock{databases_mutex};
    auto it = db_uuid_map.find(uuid);
    if (it == db_uuid_map.end())
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database UUID {} does not exist", toString(uuid));
    return it->second;
}

DatabasePtr DatabaseCatalog::tryGetDatabase(const UUID & uuid) const
{
    assert(uuid != UUIDHelpers::Nil);
    std::lock_guard lock{databases_mutex};
    auto it = db_uuid_map.find(uuid);
    if (it == db_uuid_map.end())
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

bool DatabaseCatalog::isTableExist(const DB::StorageID & table_id, ContextPtr context_) const
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
    return db && db->isTableExist(table_id.table_name, context_);
}

void DatabaseCatalog::assertTableDoesntExist(const StorageID & table_id, ContextPtr context_) const
{
    if (isTableExist(table_id, context_))
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

void DatabaseCatalog::addUUIDMapping(const UUID & uuid)
{
    addUUIDMapping(uuid, nullptr, nullptr);
}

void DatabaseCatalog::addUUIDMapping(const UUID & uuid, const DatabasePtr & database, const StoragePtr & table)
{
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    assert((database && table) || (!database && !table));
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto [it, inserted] = map_part.map.try_emplace(uuid, database, table);
    if (inserted)
        return;

    auto & prev_database = it->second.first;
    auto & prev_table = it->second.second;
    assert((prev_database && prev_table) || (!prev_database && !prev_table));

    if (!prev_table && table)
    {
        /// It's empty mapping, it was created to "lock" UUID and prevent collision. Just update it.
        prev_database = database;
        prev_table = table;
        return;
    }

    /// We are trying to replace existing mapping (prev_table != nullptr), it's logical error
    if (table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapping for table with UUID={} already exists", toString(uuid));
    /// Normally this should never happen, but it's possible when the same UUIDs are explicitly specified in different CREATE queries,
    /// so it's not LOGICAL_ERROR
    throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Mapping for table with UUID={} already exists. It happened due to UUID collision, "
                    "most likely because some not random UUIDs were manually specified in CREATE queries.", toString(uuid));
}

void DatabaseCatalog::removeUUIDMapping(const UUID & uuid)
{
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto it = map_part.map.find(uuid);
    if (it == map_part.map.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapping for table with UUID={} doesn't exist", toString(uuid));
    it->second = {};
}

void DatabaseCatalog::removeUUIDMappingFinally(const UUID & uuid)
{
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    if (!map_part.map.erase(uuid))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapping for table with UUID={} doesn't exist", toString(uuid));
}

void DatabaseCatalog::updateUUIDMapping(const UUID & uuid, DatabasePtr database, StoragePtr table)
{
    assert(uuid != UUIDHelpers::Nil && getFirstLevelIdx(uuid) < uuid_map.size());
    assert(database && table);
    UUIDToStorageMapPart & map_part = uuid_map[getFirstLevelIdx(uuid)];
    std::lock_guard lock{map_part.mutex};
    auto it = map_part.map.find(uuid);
    if (it == map_part.map.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapping for table with UUID={} doesn't exist", toString(uuid));
    auto & prev_database = it->second.first;
    auto & prev_table = it->second.second;
    assert(prev_database && prev_table);
    prev_database = std::move(database);
    prev_table = std::move(table);
}

std::unique_ptr<DatabaseCatalog> DatabaseCatalog::database_catalog;

DatabaseCatalog::DatabaseCatalog(ContextMutablePtr global_context_)
    : WithMutableContext(global_context_), log(&Poco::Logger::get("DatabaseCatalog"))
{
    TemporaryLiveViewCleaner::init(global_context_);
}

DatabaseCatalog & DatabaseCatalog::init(ContextMutablePtr global_context_)
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

DatabasePtr DatabaseCatalog::getDatabase(const String & database_name, ContextPtr local_context) const
{
    String resolved_database = local_context->resolveDatabase(database_name);
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

DDLGuardPtr DatabaseCatalog::getDDLGuard(const String & database, const String & table)
{
    std::unique_lock lock(ddl_guards_mutex);
    auto db_guard_iter = ddl_guards.try_emplace(database).first;
    DatabaseGuard & db_guard = db_guard_iter->second;
    return std::make_unique<DDLGuard>(db_guard.first, db_guard.second, std::move(lock), table, database);
}

std::unique_lock<std::shared_mutex> DatabaseCatalog::getExclusiveDDLGuardForDatabase(const String & database)
{
    DDLGuards::iterator db_guard_iter;
    {
        std::unique_lock lock(ddl_guards_mutex);
        db_guard_iter = ddl_guards.try_emplace(database).first;
        assert(db_guard_iter->second.first.contains(""));
    }
    DatabaseGuard & db_guard = db_guard_iter->second;
    return std::unique_lock{db_guard.second};
}

bool DatabaseCatalog::isDictionaryExist(const StorageID & table_id) const
{
    auto storage = tryGetTable(table_id, getContext());
    bool storage_is_dictionary = storage && storage->isDictionary();

    return storage_is_dictionary;
}

StoragePtr DatabaseCatalog::getTable(const StorageID & table_id, ContextPtr local_context) const
{
    std::optional<Exception> exc;
    auto res = getTableImpl(table_id, local_context, &exc);
    if (!res.second)
        throw Exception(*exc);
    return res.second;
}

StoragePtr DatabaseCatalog::tryGetTable(const StorageID & table_id, ContextPtr local_context) const
{
    return getTableImpl(table_id, local_context, nullptr).second;
}

DatabaseAndTable DatabaseCatalog::getDatabaseAndTable(const StorageID & table_id, ContextPtr local_context) const
{
    std::optional<Exception> exc;
    auto res = getTableImpl(table_id, local_context, &exc);
    if (!res.second)
        throw Exception(*exc);
    return res;
}

DatabaseAndTable DatabaseCatalog::tryGetDatabaseAndTable(const StorageID & table_id, ContextPtr local_context) const
{
    return getTableImpl(table_id, local_context, nullptr);
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
    String path = getContext()->getPath() + "metadata_dropped/";

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

    LOG_INFO(log, "Found {} partially dropped tables. Will load them and retry removal.", dropped_metadata.size());

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
    return getContext()->getPath() + "metadata_dropped/" +
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
    {
        drop_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        table->is_dropped = true;
    }
    else
    {
        /// Try load table from metadata to drop it correctly (e.g. remove metadata from zk or remove data from all volumes)
        LOG_INFO(log, "Trying load partially dropped table {} from {}", table_id.getNameForLogs(), dropped_metadata_path);
        ASTPtr ast = DatabaseOnDisk::parseQueryFromMetadata(
            log, getContext(), dropped_metadata_path, /*throw_on_error*/ false, /*remove_empty*/ false);
        auto * create = typeid_cast<ASTCreateQuery *>(ast.get());
        assert(!create || create->uuid == table_id.uuid);

        if (create)
        {
            String data_path = "store/" + getPathForUUID(table_id.uuid);
            create->setDatabase(table_id.database_name);
            create->setTable(table_id.table_name);
            try
            {
                table = createTableFromAST(*create, table_id.getDatabaseName(), data_path, getContext(), false).second;
                table->is_dropped = true;
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

        addUUIDMapping(table_id.uuid);
        drop_time = FS::getModificationTime(dropped_metadata_path);
    }

    std::lock_guard lock(tables_marked_dropped_mutex);
    if (ignore_delay)
        tables_marked_dropped.push_front({table_id, table, dropped_metadata_path, drop_time});
    else
        tables_marked_dropped.push_back({table_id, table, dropped_metadata_path, drop_time + drop_delay_sec});
    tables_marked_dropped_ids.insert(table_id.uuid);
    CurrentMetrics::add(CurrentMetrics::TablesToDropQueueSize, 1);

    /// If list of dropped tables was empty, start a drop task.
    /// If ignore_delay is set, schedule drop task as soon as possible.
    if (drop_task && (tables_marked_dropped.size() == 1 || ignore_delay))
        (*drop_task)->schedule();
}

void DatabaseCatalog::dropTableDataTask()
{
    /// Background task that removes data of tables which were marked as dropped by Atomic databases.
    /// Table can be removed when it's not used by queries and drop_delay_sec elapsed since it was marked as dropped.

    bool need_reschedule = true;
    /// Default reschedule time for the case when we are waiting for reference count to become 1.
    size_t schedule_after_ms = reschedule_time_ms;
    TableMarkedAsDropped table;
    try
    {
        std::lock_guard lock(tables_marked_dropped_mutex);
        assert(!tables_marked_dropped.empty());
        time_t current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        time_t min_drop_time = std::numeric_limits<time_t>::max();
        size_t tables_in_use_count = 0;
        auto it = std::find_if(tables_marked_dropped.begin(), tables_marked_dropped.end(), [&](const auto & elem)
        {
            bool not_in_use = !elem.table || elem.table.unique();
            bool old_enough = elem.drop_time <= current_time;
            min_drop_time = std::min(min_drop_time, elem.drop_time);
            tables_in_use_count += !not_in_use;
            return not_in_use && old_enough;
        });
        if (it != tables_marked_dropped.end())
        {
            table = std::move(*it);
            LOG_INFO(log, "Have {} tables in drop queue ({} of them are in use), will try drop {}",
                     tables_marked_dropped.size(), tables_in_use_count, table.table_id.getNameForLogs());
            tables_marked_dropped.erase(it);
            /// Schedule the task as soon as possible, while there are suitable tables to drop.
            schedule_after_ms = 0;
        }
        else if (current_time < min_drop_time)
        {
            /// We are waiting for drop_delay_sec to exceed, no sense to wakeup until min_drop_time.
            /// If new table is added to the queue with ignore_delay flag, schedule() is called to wakeup the task earlier.
            schedule_after_ms = (min_drop_time - current_time) * 1000;
            LOG_TRACE(log, "Not found any suitable tables to drop, still have {} tables in drop queue ({} of them are in use). "
                           "Will check again after {} seconds", tables_marked_dropped.size(), tables_in_use_count, min_drop_time - current_time);
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
            std::lock_guard lock(tables_marked_dropped_mutex);
            [[maybe_unused]] auto removed = tables_marked_dropped_ids.erase(table.table_id.uuid);
            assert(removed);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot drop table " + table.table_id.getNameForLogs() +
                                        ". Will retry later.");
            {
                table.drop_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) + drop_error_cooldown_sec;
                std::lock_guard lock(tables_marked_dropped_mutex);
                tables_marked_dropped.emplace_back(std::move(table));
                /// If list of dropped tables was empty, schedule a task to retry deletion.
                if (tables_marked_dropped.size() == 1)
                {
                    need_reschedule = true;
                    schedule_after_ms = drop_error_cooldown_sec * 1000;
                }
            }
        }

        wait_table_finally_dropped.notify_all();
    }

    /// Do not schedule a task if there is no tables to drop
    if (need_reschedule)
        (*drop_task)->scheduleAfter(schedule_after_ms);
}

void DatabaseCatalog::dropTableFinally(const TableMarkedAsDropped & table)
{
    if (table.table)
    {
        table.table->drop();
    }

    /// Even if table is not loaded, try remove its data from disk.
    /// TODO remove data from all volumes
    fs::path data_path = fs::path(getContext()->getPath()) / "store" / getPathForUUID(table.table_id.uuid);
    if (fs::exists(data_path))
    {
        LOG_INFO(log, "Removing data directory {} of dropped table {}", data_path.string(), table.table_id.getNameForLogs());
        fs::remove_all(data_path);
    }

    LOG_INFO(log, "Removing metadata {} of dropped table {}", table.metadata_path, table.table_id.getNameForLogs());
    fs::remove(fs::path(table.metadata_path));

    removeUUIDMappingFinally(table.table_id.uuid);
    CurrentMetrics::sub(CurrentMetrics::TablesToDropQueueSize, 1);
}

String DatabaseCatalog::getPathForUUID(const UUID & uuid)
{
    const size_t uuid_prefix_len = 3;
    return toString(uuid).substr(0, uuid_prefix_len) + '/' + toString(uuid) + '/';
}

void DatabaseCatalog::waitTableFinallyDropped(const UUID & uuid)
{
    if (uuid == UUIDHelpers::Nil)
        return;

    LOG_DEBUG(log, "Waiting for table {} to be finally dropped", toString(uuid));
    std::unique_lock lock{tables_marked_dropped_mutex};
    wait_table_finally_dropped.wait(lock, [&]()
    {
        return !tables_marked_dropped_ids.contains(uuid);
    });
}

void DatabaseCatalog::addLoadingDependencies(const QualifiedTableName & table, TableNamesSet && dependencies)
{
    DependenciesInfos new_info;
    for (const auto & dependency : dependencies)
        new_info[dependency].dependent_database_objects.insert(table);
    new_info[table].dependencies = std::move(dependencies);
    addLoadingDependencies(new_info);
}

void DatabaseCatalog::addLoadingDependencies(const DependenciesInfos & new_infos)
{
    std::lock_guard lock{databases_mutex};
    mergeDependenciesGraphs(loading_dependencies, new_infos);
}

DependenciesInfo DatabaseCatalog::getLoadingDependenciesInfo(const StorageID & table_id) const
{
    std::lock_guard lock{databases_mutex};
    auto it = loading_dependencies.find(table_id.getQualifiedName());
    if (it == loading_dependencies.end())
        return {};
    return it->second;
}

TableNamesSet DatabaseCatalog::tryRemoveLoadingDependencies(const StorageID & table_id, bool check_dependencies, bool is_drop_database)
{
    QualifiedTableName removing_table = table_id.getQualifiedName();
    std::lock_guard lock{databases_mutex};
    return tryRemoveLoadingDependenciesUnlocked(removing_table, check_dependencies, is_drop_database);
}

TableNamesSet DatabaseCatalog::tryRemoveLoadingDependenciesUnlocked(const QualifiedTableName & removing_table, bool check_dependencies, bool is_drop_database)
{
    auto it = loading_dependencies.find(removing_table);
    if (it == loading_dependencies.end())
        return {};

    TableNamesSet & dependent = it->second.dependent_database_objects;
    if (!dependent.empty())
    {
        if (check_dependencies && !is_drop_database)
            throw Exception(ErrorCodes::HAVE_DEPENDENT_OBJECTS, "Cannot drop or rename {}, because some tables depend on it: {}",
                            removing_table, fmt::join(dependent, ", "));

        /// For DROP DATABASE we should ignore dependent tables from the same database.
        /// TODO unload tables in reverse topological order and remove this code
        if (check_dependencies)
        {
            TableNames from_other_databases;
            for (const auto & table : dependent)
                if (table.database != removing_table.database)
                    from_other_databases.push_back(table);

            if (!from_other_databases.empty())
                throw Exception(ErrorCodes::HAVE_DEPENDENT_OBJECTS, "Cannot drop or rename {}, because some tables depend on it: {}",
                                removing_table, fmt::join(from_other_databases, ", "));
        }

        for (const auto & table : dependent)
        {
            [[maybe_unused]] bool removed = loading_dependencies[table].dependencies.erase(removing_table);
            assert(removed);
        }
        dependent.clear();
    }

    TableNamesSet dependencies = it->second.dependencies;
    for (const auto & table : dependencies)
    {
        [[maybe_unused]] bool removed = loading_dependencies[table].dependent_database_objects.erase(removing_table);
        assert(removed);
    }

    loading_dependencies.erase(it);
    return dependencies;
}

void DatabaseCatalog::checkTableCanBeRemovedOrRenamed(const StorageID & table_id) const
{
    QualifiedTableName removing_table = table_id.getQualifiedName();
    std::lock_guard lock{databases_mutex};
    auto it = loading_dependencies.find(removing_table);
    if (it == loading_dependencies.end())
        return;

    const TableNamesSet & dependent = it->second.dependent_database_objects;
    if (!dependent.empty())
        throw Exception(ErrorCodes::HAVE_DEPENDENT_OBJECTS, "Cannot drop or rename {}, because some tables depend on it: {}",
                            table_id.getNameForLogs(), fmt::join(dependent, ", "));
}

void DatabaseCatalog::updateLoadingDependencies(const StorageID & table_id, TableNamesSet && new_dependencies)
{
    if (new_dependencies.empty())
        return;
    QualifiedTableName table_name = table_id.getQualifiedName();
    std::lock_guard lock{databases_mutex};
    auto it = loading_dependencies.find(table_name);
    if (it == loading_dependencies.end())
        it = loading_dependencies.emplace(table_name, DependenciesInfo{}).first;

    auto & old_dependencies = it->second.dependencies;
    for (const auto & dependency : old_dependencies)
        if (!new_dependencies.contains(dependency))
            loading_dependencies[dependency].dependent_database_objects.erase(table_name);

    for (const auto & dependency : new_dependencies)
        if (!old_dependencies.contains(dependency))
            loading_dependencies[dependency].dependent_database_objects.insert(table_name);

    old_dependencies = std::move(new_dependencies);
}


DDLGuard::DDLGuard(Map & map_, std::shared_mutex & db_mutex_, std::unique_lock<std::mutex> guards_lock_, const String & elem, const String & database_name)
        : map(map_), db_mutex(db_mutex_), guards_lock(std::move(guards_lock_))
{
    it = map.emplace(elem, Entry{std::make_unique<std::mutex>(), 0}).first;
    ++it->second.counter;
    guards_lock.unlock();
    table_lock = std::unique_lock(*it->second.mutex);
    is_database_guard = elem.empty();
    if (!is_database_guard)
    {

        bool locked_database_for_read = db_mutex.try_lock_shared();
        if (!locked_database_for_read)
        {
            releaseTableLock();
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} is currently dropped or renamed", database_name);
        }
    }
}

void DDLGuard::releaseTableLock() noexcept
{
    if (table_lock_removed)
        return;

    table_lock_removed = true;
    guards_lock.lock();
    UInt32 counter = --it->second.counter;
    table_lock.unlock();
    if (counter == 0)
        map.erase(it);
    guards_lock.unlock();
}

DDLGuard::~DDLGuard()
{
    if (!is_database_guard)
        db_mutex.unlock_shared();
    releaseTableLock();
}

}
