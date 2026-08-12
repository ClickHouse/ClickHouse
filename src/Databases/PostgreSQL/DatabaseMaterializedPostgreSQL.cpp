#include <Databases/PostgreSQL/DatabaseMaterializedPostgreSQL.h>
#include <Storages/PostgreSQL/MaterializedPostgreSQLSettings.h>

#if USE_LIBPQXX

#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>

#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>
#include <Common/Macros.h>
#include <Common/PoolId.h>
#include <Common/parseAddress.h>
#include <Common/parseRemoteDescription.h>
#include <Common/RemoteHostFilter.h>
#include <Common/AsyncLoader.h>
#include <Common/FailPoint.h>
#include <IO/WriteHelpers.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseFactory.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StoragePostgreSQL.h>
#include <Storages/AlterCommands.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Common/escapeForFileName.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 postgresql_connection_attempt_timeout;
}

namespace MaterializedPostgreSQLSetting
{
    extern const MaterializedPostgreSQLSettingsString materialized_postgresql_tables_list;
}

namespace FailPoints
{
    extern const char database_materialized_postgresql_pause_before_table_drop[];
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_NOT_ALLOWED;
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int CANNOT_BACKUP_TABLE;
}

DatabaseMaterializedPostgreSQL::DatabaseMaterializedPostgreSQL(
        ContextPtr context_,
        const String & metadata_path_,
        UUID uuid_,
        bool is_attach_,
        const String & database_name_,
        const String & postgres_database_name,
        const postgres::ConnectionInfo & connection_info_,
        std::unique_ptr<MaterializedPostgreSQLSettings> settings_)
    : DatabaseAtomic(database_name_, metadata_path_, uuid_, "DatabaseMaterializedPostgreSQL (" + database_name_ + ")", context_)
    , is_attach(is_attach_)
    , remote_database_name(postgres_database_name)
    , connection_info(connection_info_)
    , settings(std::move(settings_))
    , startup_task(getContext()->getSchedulePool()->createTask(StorageID::createEmpty(), "MaterializedPostgreSQLDatabaseStartup", [this]{ tryStartSynchronization(); }))
{
}

void DatabaseMaterializedPostgreSQL::tryStartSynchronization()
{
    if (shutdown_called)
        return;

    try
    {
        startSynchronization();
        LOG_INFO(log, "Successfully loaded tables from PostgreSQL and started replication");
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to start replication from PostgreSQL, "
                  "will retry. Error: {}", getCurrentExceptionMessage(true));

        if (!shutdown_called)
            startup_task->scheduleAfter(5000);
    }
}

void DatabaseMaterializedPostgreSQL::startSynchronization()
{
    std::lock_guard lock(handler_mutex);
    if (shutdown_called)
        return;

    replication_handler = std::make_unique<PostgreSQLReplicationHandler>(
            remote_database_name,
            /* table_name */"",
            TSA_SUPPRESS_WARNING_FOR_READ(database_name),     /// FIXME
            toString(getUUID()),
            connection_info,
            getContext(),
            is_attach,
            *settings,
            /* is_materialized_postgresql_database = */ true);

    std::set<String> tables_to_replicate;
    try
    {
        tables_to_replicate = replication_handler->fetchRequiredTables();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        LOG_ERROR(log, "Unable to load replicated tables list");
        throw;
    }

    if (tables_to_replicate.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Got empty list of tables to replicate");

    /// Build the wrappers into a local map and publish them in one go below, so that `tables_mutex`
    /// is only held for the assignment: `DatabaseAtomic::tryGetTable` takes the base class mutex,
    /// and nesting that inside `tables_mutex` would introduce a second lock order.
    std::map<std::string, StoragePtr> new_materialized_tables;

    for (const auto & table_name : tables_to_replicate)
    {
        /// Check nested ReplacingMergeTree table.
        auto storage = DatabaseAtomic::tryGetTable(table_name, getContext());

        if (storage)
        {
            /// Nested table was already created and synchronized.
            storage = std::make_shared<StorageMaterializedPostgreSQL>(storage, getContext(), remote_database_name, table_name);
        }
        else
        {
            /// Nested table does not exist and will be created by replication thread.
            /// FIXME TSA
            storage = std::make_shared<StorageMaterializedPostgreSQL>(StorageID(TSA_SUPPRESS_WARNING_FOR_READ(database_name), table_name), getContext(), remote_database_name, table_name);
        }

        /// Cache MaterializedPostgreSQL wrapper over nested table.
        new_materialized_tables[table_name] = storage;

        /// Let replication thread know, which tables it needs to keep in sync.
        replication_handler->addStorage(table_name, storage->as<StorageMaterializedPostgreSQL>());
    }

    LOG_TRACE(log, "Loaded {} tables. Starting synchronization", new_materialized_tables.size());

    {
        std::lock_guard tables_lock(tables_mutex);
        materialized_tables = std::move(new_materialized_tables);
    }

    replication_handler->startup(/* delayed */false);
}


LoadTaskPtr DatabaseMaterializedPostgreSQL::startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode)
{
    auto base = DatabaseAtomic::startupDatabaseAsync(async_loader, std::move(startup_after), mode);
    auto job = makeLoadJob(
        base->goals(),
        TablesLoaderBackgroundStartupPoolId,
        fmt::format("startup MaterializedPostgreSQL database {}", getDatabaseName()),
        [this] (AsyncLoader &, const LoadJobPtr &)
        {
            startup_task->activateAndSchedule();
        });
    std::scoped_lock lock(mutex);
    return startup_postgresql_database_task = makeLoadTask(async_loader, {job});
}

void DatabaseMaterializedPostgreSQL::waitDatabaseStarted() const
{
    LoadTaskPtr task;
    {
        std::scoped_lock lock(mutex);
        task = startup_postgresql_database_task;
    }
    if (task)
        waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), task);
}

void DatabaseMaterializedPostgreSQL::stopLoading()
{
    LoadTaskPtr stop_startup_postgresql_database;
    {
        std::scoped_lock lock(mutex);
        stop_startup_postgresql_database.swap(startup_postgresql_database_task);
    }
    stop_startup_postgresql_database.reset();
    DatabaseAtomic::stopLoading();
}

void DatabaseMaterializedPostgreSQL::applySettingsChanges(const SettingsChanges & settings_changes, ContextPtr query_context)
{
    std::lock_guard lock(handler_mutex);

    /// Validate the whole list before applying anything. Otherwise a rejected change (an unknown,
    /// immutable, or not-allowed setting) combined with an accepted one in the same statement could
    /// leave the database partially modified: the accepted change would already be applied to the live
    /// `replication_handler` and in-memory `settings` while the rejected change aborts the statement
    /// before the on-disk metadata is updated, so the live state would diverge from the metadata until
    /// the next restart. Checking everything first makes the `ALTER DATABASE ... MODIFY SETTING` atomic.
    for (const auto & change : settings_changes)
    {
        if (!settings->has(change.name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine {} does not support setting `{}`", getEngineName(), change.name);

        if (change.name == "materialized_postgresql_tables_list")
        {
            if (!query_context->isInternalQuery())
                throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Changing setting `{}` is not allowed", change.name);
        }
        else if (change.name == "materialized_postgresql_use_extended_date_and_time_types")
        {
            throw Exception(ErrorCodes::QUERY_NOT_ALLOWED,
                            "Setting `{}` only controls the column types chosen when the nested tables are created "
                            "by type inference, and cannot be changed for an existing database: the already created "
                            "nested tables keep their fixed column types. Recreate the database to change it.", change.name);
        }
        else if ((change.name != "materialized_postgresql_allow_automatic_update") && (change.name != "materialized_postgresql_max_block_size"))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting");
        }
    }

    /// All changes are valid; apply them.
    bool need_update_on_disk = false;
    for (const auto & change : settings_changes)
    {
        if (change.name == "materialized_postgresql_tables_list")
        {
            need_update_on_disk = true;
        }
        else if ((change.name == "materialized_postgresql_allow_automatic_update") || (change.name == "materialized_postgresql_max_block_size"))
        {
            replication_handler->setSetting(change);
            need_update_on_disk = true;
        }

        settings->applyChange(change);
    }

    if (need_update_on_disk)
        DatabaseOnDisk::modifySettingsMetadata(settings_changes, query_context);
}


StoragePtr DatabaseMaterializedPostgreSQL::tryGetTable(const String & name, ContextPtr local_context) const
{
    /// In order to define which table access is needed - to MaterializedPostgreSQL table (only in case of SELECT queries) or
    /// to its nested ReplacingMergeTree table (in all other cases), the context of a query is modified.
    ///
    /// Note: In select query we call MaterializedPostgreSQL table and it calls tryGetTable from its nested.
    /// So the only point, where synchronization is needed - access to MaterializedPostgreSQL table wrapper over nested table.
    /// The emptiness check belongs inside the critical section as well: reading it without the lock
    /// races with `stopReplication`, which clears the map.
    if (!local_context->isInternalQuery())
    {
        bool wrap_nested = false;
        {
            std::lock_guard lock(tables_mutex);
            if (!materialized_tables.empty())
            {
                auto table = materialized_tables.find(name);

                /// Return wrapper over ReplacingMergeTree table. If table synchronization just started, table will not
                /// be accessible immediately. Table is considered to exist once its nested table was created.
                if (table != materialized_tables.end() && table->second->as <StorageMaterializedPostgreSQL>()->hasNested())
                    return table->second;

                return StoragePtr{};
            }

            /// The map is empty in two different states. After `stopReplication` (server shutdown or
            /// `DROP DATABASE`) all access goes to the nested tables. But it is also empty right after
            /// `CREATE` / `ATTACH DATABASE` and after a server restart, until `startSynchronization`
            /// has fetched the tables list from PostgreSQL and published the wrappers - including the
            /// whole time that first connection attempt is failing and retrying. In that startup
            /// window a user-facing read must not fall back to the nested table - that would bypass
            /// the forced `FINAL` and the `_sign = 1` filter and expose stale and deleted row
            /// versions - so wrap the nested table on the fly instead.
            wrap_nested = !replication_stopped;
        }

        if (wrap_nested)
        {
            /// `tables_mutex` is released: the base class lookup takes its own mutex, and taking it
            /// while holding `tables_mutex` would introduce a second lock order. If
            /// `startSynchronization` publishes the wrappers concurrently, the wrapper built here is
            /// equivalent to the published one.
            auto nested = DatabaseAtomic::tryGetTable(name, local_context);
            if (!nested)
                return StoragePtr{};

            return std::make_shared<StorageMaterializedPostgreSQL>(nested, getContext(), remote_database_name, name);
        }
    }

    /// `tables_mutex` is released before the call: the base class takes its own mutex, and taking it
    /// while holding `tables_mutex` would introduce a second lock order.
    return DatabaseAtomic::tryGetTable(name, local_context);
}


/// `except` is not empty in case it is detach and it will contain only one table name - name of detached table.
/// In case we have a user defined setting `materialized_postgresql_tables_list`, then list of tables is always taken there.
/// Otherwise we traverse materialized storages to find out the list.
String DatabaseMaterializedPostgreSQL::getFormattedTablesList(const String & except) const
{
    String tables_list;
    for (const auto & table : materialized_tables)
    {
        if (table.first == except)
            continue;

        if (!tables_list.empty())
            tables_list += ',';

        tables_list += table.first;
    }
    return tables_list;
}


ASTPtr DatabaseMaterializedPostgreSQL::getCreateTableQueryImpl(const String & table_name, ContextPtr local_context, bool throw_on_error) const
{
    if (!local_context->hasQueryContext())
        return DatabaseAtomic::getCreateTableQueryImpl(table_name, local_context, throw_on_error);

    /// Use the table's actual (stable) UUID rather than a freshly generated one. Otherwise every
    /// call to this method returns a slightly different definition, which breaks operations that
    /// read the table metadata several times and expect it to stay consistent - most notably
    /// BACKUP, which kept retrying with "Table ... was created or changed its definition during
    /// scanning" and never finished. Fall back to a generated UUID only if the table does not exist
    /// yet. Fetched before acquiring `handler_mutex` to avoid lock ordering issues.
    UUID table_uuid = UUIDHelpers::generateV4();
    if (auto existing_table = DatabaseAtomic::tryGetTable(table_name, getContext()))
        table_uuid = existing_table->getStorageID().uuid;

    std::lock_guard lock(handler_mutex);

    ASTPtr ast_storage;
    try
    {
        auto storage = std::make_shared<StorageMaterializedPostgreSQL>(StorageID(TSA_SUPPRESS_WARNING_FOR_READ(database_name), table_name), getContext(), remote_database_name, table_name);
        ast_storage = replication_handler->getCreateNestedTableQuery(storage.get(), table_name);
        assert_cast<ASTCreateQuery *>(ast_storage.get())->uuid = table_uuid;
    }
    catch (...)
    {
        if (throw_on_error)
        {
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY,
                            "Received error while fetching table structure for table {} from PostgreSQL: {}",
                            backQuote(table_name), getCurrentExceptionMessage(true));
        }

        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return ast_storage;
}


ASTPtr DatabaseMaterializedPostgreSQL::createAlterSettingsQuery(const SettingChange & new_setting)
{
    auto set = make_intrusive<ASTSetQuery>();
    set->is_standalone = false;
    set->changes = {new_setting};

    auto command = make_intrusive<ASTAlterCommand>();
    command->type = ASTAlterCommand::Type::MODIFY_DATABASE_SETTING;
    command->settings_changes = command->children.emplace_back(std::move(set)).get();

    auto command_list = make_intrusive<ASTExpressionList>();
    command_list->children.push_back(command);

    auto query = make_intrusive<ASTAlterQuery>();
    auto * alter = query->as<ASTAlterQuery>();

    alter->alter_object = ASTAlterQuery::AlterObjectType::DATABASE;
    alter->setDatabase(TSA_SUPPRESS_WARNING_FOR_READ(database_name));     /// FIXME
    alter->set(alter->command_list, command_list);

    return query;
}


void DatabaseMaterializedPostgreSQL::createTable(ContextPtr local_context, const String & table_name, const StoragePtr & table, const ASTPtr & query)
{
    /// Create table query can only be called from replication thread.
    if (local_context->isInternalQuery())
    {
        DatabaseAtomic::createTable(local_context, table_name, table, query);
        return;
    }

    const auto & create = query->as<ASTCreateQuery>();
    if (!create->attach)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED,
                        "CREATE TABLE is not allowed for database engine {}. Use ATTACH TABLE instead", getEngineName());

    /// Create ReplacingMergeTree table.
    auto query_copy = query->clone();
    auto * create_query = assert_cast<ASTCreateQuery *>(query_copy.get());
    create_query->attach = false;
    create_query->attach_short_syntax = false;
    DatabaseCatalog::instance().addUUIDMapping(create->uuid);
    DatabaseAtomic::createTable(StorageMaterializedPostgreSQL::makeNestedTableContext(local_context), table_name, table, query_copy);

    /// Attach MaterializedPostgreSQL table.
    attachTable(local_context, table_name, table, {});
}


void DatabaseMaterializedPostgreSQL::attachTable(ContextPtr context_, const String & table_name, const StoragePtr & table, const String & relative_table_path)
{
    /// If there is query context then we need to attach materialized storage.
    /// If there is no query context then we need to attach internal storage from atomic database.
    if (CurrentThread::isInitialized() && CurrentThread::get().tryGetQueryContext())
    {
        auto current_context = Context::createCopy(getContext()->getGlobalContext());
        current_context->setInternalQuery(true);

        /// We just came from createTable() and created nested table there. Add assert.
        auto nested_table = DatabaseAtomic::tryGetTable(table_name, current_context);
        chassert(nested_table != nullptr);

        try
        {
            auto tables_to_replicate = (*settings)[MaterializedPostgreSQLSetting::materialized_postgresql_tables_list].value;
            if (tables_to_replicate.empty())
            {
                std::lock_guard tables_lock(tables_mutex);
                tables_to_replicate = getFormattedTablesList();
            }

            /// tables_to_replicate can be empty if postgres database had no tables when this database was created.
            SettingChange new_setting("materialized_postgresql_tables_list", tables_to_replicate.empty() ? table_name : (tables_to_replicate + "," + table_name));
            auto alter_query = createAlterSettingsQuery(new_setting);

            /// Executed without `tables_mutex`: the ALTER reaches `applySettingsChanges`, which takes
            /// `handler_mutex`, and the two mutexes must always be taken in that order.
            InterpreterAlterQuery(alter_query, current_context).execute();

            auto storage = std::make_shared<StorageMaterializedPostgreSQL>(table, getContext(), remote_database_name, table_name);
            {
                std::lock_guard tables_lock(tables_mutex);
                materialized_tables[table_name] = storage;
            }

            std::lock_guard lock(handler_mutex);
            replication_handler->addTableToReplication(dynamic_cast<StorageMaterializedPostgreSQL *>(storage.get()), table_name);
        }
        catch (...)
        {
            /// This is a failed attach table. Remove already created nested table.
            DatabaseAtomic::dropTable(current_context, table_name, true);
            throw;
        }
    }
    else
    {
        DatabaseAtomic::attachTable(context_, table_name, table, relative_table_path);
    }
}

StoragePtr DatabaseMaterializedPostgreSQL::detachTable(ContextPtr, const String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DETACH TABLE not allowed, use DETACH PERMANENTLY");
}

void DatabaseMaterializedPostgreSQL::detachTablePermanently(ContextPtr, const String & table_name)
{
    /// If there is query context then we need to detach materialized storage.
    /// If there is no query context then we need to detach internal storage from atomic database.
    if (CurrentThread::isInitialized() && CurrentThread::get().tryGetQueryContext())
    {
        StoragePtr table_to_delete;
        String tables_to_replicate;
        {
            std::lock_guard tables_lock(tables_mutex);

            /// Look the table up instead of using `materialized_tables[table_name]`: the latter inserts
            /// an empty entry for an unknown name, and that null `StoragePtr` stayed in the map after
            /// the exception below, so a later `tryGetTable` would dereference it.
            auto it = materialized_tables.find(table_name);
            if (it == materialized_tables.end() || !it->second)
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "Materialized table `{}` does not exist", table_name);

            table_to_delete = it->second;
            tables_to_replicate = getFormattedTablesList(table_name);
        }

        /// tables_to_replicate can be empty if postgres database had no tables when this database was created.
        SettingChange new_setting("materialized_postgresql_tables_list", tables_to_replicate);
        auto alter_query = createAlterSettingsQuery(new_setting);

        {
            auto current_context = Context::createCopy(getContext()->getGlobalContext());
            current_context->setInternalQuery(true);
            InterpreterAlterQuery(alter_query, current_context).execute();
        }

        auto nested = table_to_delete->as<StorageMaterializedPostgreSQL>()->getNested();
        if (!nested)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Inner table `{}` does not exist", table_name);

        std::lock_guard lock(handler_mutex);
        replication_handler->removeTableFromReplication(table_name);

        try
        {
            auto current_context = Context::createCopy(getContext()->getGlobalContext());
            current_context->makeQueryContext();
            DatabaseAtomic::dropTable(current_context, table_name, true);
        }
        catch (Exception & e)
        {
            /// We already removed this table from replication and adding it back will be an overkill..
            /// TODO: this is bad, we leave a table lying somewhere not dropped, and if user will want
            /// to move it back into replication, he will fail to do so because there is undropped nested with the same name.
            /// This can also happen if we crash after removing table from replication and before dropping nested.
            /// As a solution, we could drop a table if it already exists and add a fresh one instead for these two cases.
            /// TODO: sounds good.
            {
                std::lock_guard tables_lock(tables_mutex);
                materialized_tables.erase(table_name);
            }

            e.addMessage("while removing table `" + table_name + "` from replication");
            throw;
        }

        {
            std::lock_guard tables_lock(tables_mutex);
            materialized_tables.erase(table_name);
        }
    }
}


void DatabaseMaterializedPostgreSQL::shutdown()
{
    shutdown_called = true;
    startup_task->deactivate();
    stopReplication();
    DatabaseAtomic::shutdown();
}


void DatabaseMaterializedPostgreSQL::stopReplication()
{
    /// Cancel the startup retry loop first: `tryStartSynchronization` keeps re-scheduling itself
    /// every 5 seconds until startup succeeds, and `InterpreterDropQuery` calls this method long
    /// before `shutdown` deactivates the task. Without this, a queued retry could re-enter
    /// `startSynchronization` and recreate `replication_handler` and the wrapper map while the
    /// database tables are being flushed and dropped. Deactivate before taking `handler_mutex`:
    /// the task body locks the same mutex, and `deactivate` waits for a running execution.
    startup_task->deactivate();

    std::lock_guard lock(handler_mutex);
    if (replication_handler)
        replication_handler->shutdown();

    /// Clear wrappers over nested, all access is not done to nested tables directly.
    /// Take the map out under `tables_mutex` and destroy the wrappers only after releasing it: readers
    /// dereference the stored `StorageMaterializedPostgreSQL` pointers while holding `tables_mutex`,
    /// so freeing a wrapper without it is a use-after-free, while running the storage destructors
    /// inside the critical section would block those readers for no reason.
    std::map<std::string, StoragePtr> tables_to_destroy;
    {
        std::lock_guard tables_lock(tables_mutex);
        /// From now on an empty map means "replication was stopped", not "startup has not finished",
        /// so user-facing reads stop wrapping the nested tables on the fly (see tryGetTable).
        replication_stopped = true;
        tables_to_destroy.swap(materialized_tables);
    }
}


void DatabaseMaterializedPostgreSQL::dropTable(ContextPtr local_context, const String & table_name, bool sync)
{
    FailPointInjection::pauseFailPoint(FailPoints::database_materialized_postgresql_pause_before_table_drop);

    /// Modify context into nested_context and pass query to Atomic database.
    DatabaseAtomic::dropTable(StorageMaterializedPostgreSQL::makeNestedTableContext(local_context), table_name, sync);
}


void DatabaseMaterializedPostgreSQL::drop(ContextPtr local_context)
{
    std::lock_guard lock(handler_mutex);
    if (replication_handler)
        replication_handler->shutdownFinal();

    DatabaseAtomic::drop(StorageMaterializedPostgreSQL::makeNestedTableContext(local_context));
}


DatabaseTablesIteratorPtr DatabaseMaterializedPostgreSQL::getTablesIterator(
    ContextPtr local_context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    /// Enumeration always exposes the physical nested ReplacingMergeTree tables, together with their
    /// Atomic UUIDs. Generic consumers of the iterator - `system.tables`, `system.parts`,
    /// `ServerAsynchronousMetrics`, backups - need the real MergeTree storages, and wrapping them
    /// here would make those tables lose their UUID and disappear from the parts and metrics views.
    ///
    /// Consumers that read the data through the iterator - `StorageMerge` - must not read the nested
    /// table directly, because that bypasses the `_sign = 1` filter and the forced `FINAL` and
    /// exposes stale and deleted row versions. They map every enumerated table through
    /// `getTableForRead` below, which hands out the same wrappers `tryGetTable` returns.
    return DatabaseAtomic::getTablesIterator(StorageMaterializedPostgreSQL::makeNestedTableContext(local_context), filter_by_table_name, skip_not_loaded);
}


StoragePtr DatabaseMaterializedPostgreSQL::getTableForRead(const String & table_name, const StoragePtr & table, ContextPtr local_context) const
{
    /// Internal queries (replication machinery, DDL, backups) work on the nested tables directly.
    if (!table || local_context->isInternalQuery())
        return table;

    /// Everything that touches `materialized_tables`, including the emptiness check, has to happen
    /// under `tables_mutex`: `stopReplication` clears the map and destroys the wrappers, so both the
    /// lookup and the `hasNested` call on a stored pointer would otherwise race with it.
    {
        std::lock_guard lock(tables_mutex);

        if (!materialized_tables.empty())
        {
            auto it = materialized_tables.find(table_name);

            /// A table is considered to exist once its nested table was created (see tryGetTable).
            if (it != materialized_tables.end() && it->second->as<StorageMaterializedPostgreSQL>()->hasNested())
                return it->second;

            /// The table is not replicated (or its nested table is not ready yet) - it is not visible
            /// to user-facing lookups either, see tryGetTable.
            return StoragePtr{};
        }

        /// After `stopReplication` (server shutdown or `DROP DATABASE`) user-facing access
        /// legitimately falls back to the nested tables, see tryGetTable.
        if (replication_stopped)
            return table;
    }

    /// Startup window: the map is empty because `startSynchronization` has not published the wrappers
    /// yet (see tryGetTable), so wrap the nested table on the fly. If `startSynchronization` publishes
    /// the wrappers concurrently, the wrapper built here is equivalent to the published one.
    return std::make_shared<StorageMaterializedPostgreSQL>(table, getContext(), remote_database_name, table_name);
}


std::vector<std::pair<ASTPtr, StoragePtr>>
DatabaseMaterializedPostgreSQL::getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const
{
    /// Fail closed instead of silently producing a partial backup. The base implementation enumerates tables
    /// through `getTablesIterator`, which (in the nested context) only sees the already-created nested
    /// ReplacingMergeTree tables. A table that this database is configured to replicate but whose nested table
    /// has not been created yet - its initial snapshot from PostgreSQL is still in progress or failed, e.g. the
    /// PostgreSQL table has no primary key and no replica identity index - would therefore be omitted from the
    /// backup without any error. Refuse the whole backup in that case, mirroring the fail-closed check in
    /// `StorageMaterializedPostgreSQL::backupData`, which the database backup path bypasses because it backs up
    /// the nested tables directly rather than the `StorageMaterializedPostgreSQL` wrappers.
    {
        std::lock_guard lock(tables_mutex);

        /// `startupDatabaseAsync` only *schedules* the background synchronization task, and `waitDatabaseStarted`
        /// returns as soon as that scheduling job has run - not once `startSynchronization` has actually populated
        /// `materialized_tables`. So right after `CREATE DATABASE` or a server restart - in particular for the
        /// whole time the initial `fetchRequiredTables` call to PostgreSQL is in flight, or while synchronization
        /// is failing and retrying - this method can run with an empty map, before any wrapper (and possibly any
        /// nested table) exists. Backing up in that window would fall through to `DatabaseAtomic::getTablesForBackup`
        /// and silently produce an empty or partial database backup. A database that finished starting up always
        /// has at least one table (`startSynchronization` refuses an empty tables list), so an empty map here means
        /// synchronization has not populated it yet: fail closed.
        if (materialized_tables.empty())
            throw Exception(
                ErrorCodes::CANNOT_BACKUP_TABLE,
                "Cannot back up database {}: it has not finished its initial synchronization from PostgreSQL yet "
                "(the list of replicated tables has not been populated - the database may have just been created, "
                "the server may have just restarted, or synchronization may be failing and retrying). Failing "
                "closed instead of producing an empty or partial database backup. Retry the backup once "
                "synchronization has populated the tables.",
                getDatabaseName());

        for (const auto & [table_name, storage] : materialized_tables)
        {
            if (filter && !filter(table_name))
                continue;

            if (!storage->as<StorageMaterializedPostgreSQL>()->hasNested())
                throw Exception(
                    ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Cannot back up table {}: its nested ReplacingMergeTree table does not exist (the table may not "
                    "have finished its initial synchronization from PostgreSQL yet). Failing closed instead of "
                    "producing a partial database backup that silently omits this table.",
                    storage->getStorageID().getNameForLogs());
        }
    }

    return DatabaseAtomic::getTablesForBackup(filter, local_context);
}

void registerDatabaseMaterializedPostgreSQL(DatabaseFactory & factory);
void registerDatabaseMaterializedPostgreSQL(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;

        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `MaterializedPostgreSQL` must have arguments");

        ASTs & engine_args = engine->arguments->children;
        const String & engine_name = engine_define->engine->name;

        StoragePostgreSQL::Configuration configuration;

        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);

        if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, args.context))
        {
            /// The `PostgreSQLSettings` are not passed: this engine does not use a connection pool,
            /// so the `postgresql_*` pool settings are rejected instead of being silently ignored.
            configuration = StoragePostgreSQL::processNamedCollectionResult(*named_collection, /*storage_settings=*/ nullptr, args.context, false);
        }
        else
        {
            if (engine_args.size() != 4)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "MaterializedPostgreSQL Database require `host:port`, `database_name`, `username`, `password`.");

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.context);

            auto parsed_host_port = parseAddress(safeGetLiteralValue<String>(engine_args[0], engine_name), 5432);

            configuration.host = parsed_host_port.first;
            configuration.port = parsed_host_port.second;
            configuration.addresses = {std::make_pair(configuration.host, configuration.port)};
            configuration.database = safeGetLiteralValue<String>(engine_args[1], engine_name);
            configuration.username = safeGetLiteralValue<String>(engine_args[2], engine_name);
            configuration.password = safeGetLiteralValue<String>(engine_args[3], engine_name);
        }

        /// An internal metadata replay (server startup / restore, the same distinction
        /// `DatabaseDataLake` uses) must keep loading whatever definition was already persisted:
        /// startup rebuilds every database from persisted metadata with an ATTACH query and
        /// `loadMetadata` aborts on the first exception, so a validation added after the database
        /// was created must not turn its stored definition into a server that cannot boot.
        const bool is_internal_metadata_replay = args.internal && args.mode >= LoadingStrictnessLevel::ATTACH;

        /// A named collection may specify the endpoint as `addresses_expr`, which fills only
        /// `configuration.addresses` and leaves `host` / `port` empty, while the connection string
        /// below is built from `host` / `port`. This engine keeps a single replication connection,
        /// so exactly one address is accepted; canonicalize it back into `host` / `port`.
        if (configuration.host.empty())
        {
            if (configuration.addresses.size() == 1)
            {
                configuration.host = configuration.addresses.front().first;
                configuration.port = configuration.addresses.front().second;
            }
            else if (!is_internal_metadata_replay)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Engine `{}` requires a single `host:port` address, but `addresses_expr` defines {} addresses",
                                engine_name, configuration.addresses.size());
            }
            /// On a replay a legacy multi-address definition keeps its historical behavior: it could
            /// be created before this validation existed (replication starts asynchronously, so the
            /// broken connection string never aborted the CREATE), and the database must keep loading
            /// with replication failing and retrying in the background rather than abort startup.
        }

        /// Enforce the server's outbound-host policy, exactly like the table engine and the table
        /// function do in `StoragePostgreSQL::getConfiguration`: a user must not be able to open a
        /// long-lived replication connection to a host that `remote_url_allow_hosts` forbids elsewhere.
        /// Skip it only for an internal metadata replay: enforcing the policy there would turn one
        /// database created before the whitelist was tightened into a server that cannot boot.
        /// A user-issued `ATTACH DATABASE` is not a replay and stays fail-closed, otherwise it
        /// would be a direct bypass of the policy.
        if (!is_internal_metadata_replay)
        {
            for (const auto & address : configuration.addresses)
                args.context->getRemoteHostFilter().checkHostAndPort(address.first, toString(address.second));
        }

        auto connection_info = postgres::formatConnectionString(
            configuration.database,
            configuration.host,
            configuration.port,
            configuration.username,
            configuration.password,
            args.context->getSettingsRef()[Setting::postgresql_connection_attempt_timeout]);

        auto postgresql_replica_settings = std::make_unique<MaterializedPostgreSQLSettings>();
        if (engine_define->settings)
            postgresql_replica_settings->loadFromQuery(*engine_define);

        return std::make_shared<DatabaseMaterializedPostgreSQL>(
            args.context, args.metadata_path, args.uuid, args.create_query.attach,
            args.database_name, configuration.database, connection_info,
            std::move(postgresql_replica_settings));
    };
    factory.registerDatabase("MaterializedPostgreSQL", create_fn, {
        .supports_arguments = true,
        .supports_settings = true,
        .supports_table_overrides = true,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::POSTGRES,
    }, Documentation{
        .description = R"DOCS_MD(
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# MaterializedPostgreSQL

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

:::note
ClickHouse Cloud users are recommended to use [ClickPipes](/integrations/clickpipes) for PostgreSQL replication to ClickHouse. This natively supports high-performance Change Data Capture (CDC) for PostgreSQL.
:::

Creates a ClickHouse database with tables from PostgreSQL database. Firstly, database with engine `MaterializedPostgreSQL` creates a snapshot of PostgreSQL database and loads required tables. Required tables can include any subset of tables from any subset of schemas from specified database. Along with the snapshot database engine acquires LSN and once initial dump of tables is performed - it starts pulling updates from WAL. After database is created, newly added tables to PostgreSQL database are not automatically added to replication. They have to be added manually with `ATTACH TABLE db.table` query.

Replication is implemented with PostgreSQL Logical Replication Protocol, which does not allow to replicate DDL, but allows to know whether replication breaking changes happened (column type changes, adding/removing columns). Such changes are detected and according tables stop receiving updates. In this case you should use `ATTACH`/ `DETACH PERMANENTLY` queries to reload table completely. If DDL does not break replication (for example, renaming a column) table will still receive updates (insertion is done by position).

:::note
This database engine is experimental. To use it, set `allow_experimental_database_materialized_postgresql` to 1 in your configuration files or by using the `SET` command:
```sql
SET allow_experimental_database_materialized_postgresql=1
```
:::

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', 'database', 'user', 'password') [SETTINGS ...]
```

**Engine Parameters**

- `host:port` — PostgreSQL server endpoint.
- `database` — PostgreSQL database name.
- `user` — PostgreSQL user.
- `password` — User password.

## Example of use {#example-of-use}

```sql
CREATE DATABASE postgres_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SHOW TABLES FROM postgres_db;

┌─name───┐
│ table1 │
└────────┘

SELECT * FROM postgres_db.postgres_table;
```

## Dynamically adding new tables to replication {#dynamically-adding-table-to-replication}

After `MaterializedPostgreSQL` database is created, it does not automatically detect new tables in according PostgreSQL database. Such tables can be added manually:

```sql
ATTACH TABLE postgres_database.new_table;
```

:::warning
Before version 22.1, adding a table to replication left a non-removed temporary replication slot (named `{db_name}_ch_replication_slot_tmp`). If attaching tables in ClickHouse version before 22.1, make sure to delete it manually (`SELECT pg_drop_replication_slot('{db_name}_ch_replication_slot_tmp')`). Otherwise disk usage will grow. This issue is fixed in 22.1.
:::

## Dynamically removing tables from replication {#dynamically-removing-table-from-replication}

It is possible to remove specific tables from replication:

```sql
DETACH TABLE postgres_database.table_to_remove PERMANENTLY;
```

## PostgreSQL schema {#schema}

PostgreSQL [schema](https://www.postgresql.org/docs/9.1/ddl-schemas.html) can be configured in 3 ways (starting from version 21.12).

1. One schema for one `MaterializedPostgreSQL` database engine. Requires to use setting `materialized_postgresql_schema`.
Tables are accessed via table name only:

```sql
CREATE DATABASE postgres_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema = 'postgres_schema';

SELECT * FROM postgres_database.table1;
```

2. Any number of schemas with specified set of tables for one `MaterializedPostgreSQL` database engine. Requires to use setting `materialized_postgresql_tables_list`. Each table is written along with its schema.
Tables are accessed via schema name and table name at the same time:

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_tables_list = 'schema1.table1,schema2.table2,schema1.table3',
         materialized_postgresql_tables_list_with_schema = 1;

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema2.table2`;
```

But in this case all tables in `materialized_postgresql_tables_list` must be written with its schema name.
Requires `materialized_postgresql_tables_list_with_schema = 1`.

Warning: for this case dots in table name are not allowed.

3. Any number of schemas with full set of tables for one `MaterializedPostgreSQL` database engine. Requires to use setting `materialized_postgresql_schema_list`.

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema_list = 'schema1,schema2,schema3';

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema1.table2`;
SELECT * FROM database1.`schema2.table2`;
```

Warning: for this case dots in table name are not allowed.

## Requirements {#requirements}

1. The [wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html) setting must have a value `logical` and `max_replication_slots` parameter must have a value at least `2` in the PostgreSQL config file.

2. Each replicated table must have one of the following [replica identity](https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY):

- primary key (by default)

- index

```bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

The primary key is always checked first. If it is absent, then the index, defined as replica identity index, is checked.
If the index is used as a replica identity, there has to be only one such index in a table.
You can check what type is used for a specific table with the following command:

```bash
postgres# SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'postgres_table'::regclass;
```

:::note
Replication of [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) values is not supported. The default value for the data type will be used.
:::

## Settings {#settings}

### `materialized_postgresql_tables_list` {#materialized-postgresql-tables-list}

Sets a comma-separated list of PostgreSQL database tables, which will be replicated via [MaterializedPostgreSQL](/reference/engines/database-engines/materialized-postgresql) database engine.

Each table can have subset of replicated columns in brackets. If subset of columns is omitted, then all columns for table will be replicated.

```sql
materialized_postgresql_tables_list = 'table1(co1, col2),table2,table3(co3, col5, col7)
```

Default value: empty list — means whole PostgreSQL database will be replicated.

### `materialized_postgresql_schema` {#materialized-postgresql-schema}

Default value: empty string. (Default schema is used)

### `materialized_postgresql_schema_list` {#materialized-postgresql-schema-list}

Default value: empty list. (Default schema is used)

### `materialized_postgresql_max_block_size` {#materialized-postgresql-max-block-size}

Sets the number of rows collected in memory before flushing data into PostgreSQL database table.

Possible values:

- Positive integer.

Default value: `65536`.

### `materialized_postgresql_replication_slot` {#materialized-postgresql-replication-slot}

A user-created replication slot. Must be used together with `materialized_postgresql_snapshot`.

### `materialized_postgresql_snapshot` {#materialized-postgresql-snapshot}

A text string identifying a snapshot, from which [initial dump of PostgreSQL tables](/reference/engines/database-engines/materialized-postgresql) will be performed. Must be used together with `materialized_postgresql_replication_slot`.

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_tables_list = 'table1,table2,table3';

SELECT * FROM database1.table1;
```

The settings can be changed, if necessary, using a DDL query. But it is impossible to change the setting `materialized_postgresql_tables_list`. To update the list of tables in this setting use the `ATTACH TABLE` query.

```sql
ALTER DATABASE postgres_database MODIFY SETTING materialized_postgresql_max_block_size = <new_size>;
```

### `materialized_postgresql_use_unique_replication_consumer_identifier` {#materialized_postgresql_use_unique_replication_consumer_identifier}

Use a unique replication consumer identifier for replication. Default: `0`.
If set to `1`, allows to setup several `MaterializedPostgreSQL` tables pointing to the same `PostgreSQL` table.

### `materialized_postgresql_use_extended_date_and_time_types` {#materialized-postgresql-use-extended-date-and-time-types}

Map the PostgreSQL `date` and `timestamp`/`timestamptz` types to ClickHouse `Date32` and `DateTime64`, which cover the wider value range of the PostgreSQL types. Default: `1`.
If set to `0`, the narrower `Date` and `DateTime` types are used instead (values outside their range or with sub-second precision are not representable).

This setting only controls the column types chosen by type inference when the nested tables are created, so it must be specified at `CREATE DATABASE` time. It cannot be changed afterwards with `ALTER DATABASE ... MODIFY SETTING` (the already created nested tables keep their fixed column types, and such a change is rejected); recreate the database to change it. It is not applicable to the `MaterializedPostgreSQL` table engine, where the column types are declared explicitly.

## Notes {#notes}

### Failover of the logical replication slot {#logical-replication-slot-failover}

Logical Replication Slots which exist on the primary are not available on standby replicas.
So if there is a failover, new primary (the old physical standby) won't be aware of any slots which were existing with old primary. This will lead to a broken replication from PostgreSQL.
A solution to this is to manage replication slots yourself and define a permanent replication slot (some information can be found [here](https://patroni.readthedocs.io/en/latest/SETTINGS.html)). You'll need to pass slot name via `materialized_postgresql_replication_slot` setting, and it has to be exported with `EXPORT SNAPSHOT` option. The snapshot identifier needs to be passed via `materialized_postgresql_snapshot` setting.

Please note that this should be used only if it is actually needed. If there is no real need for that or full understanding why, then it is better to allow the table engine to create and manage its own replication slot.

**Example (from [@bchrobot](https://github.com/bchrobot))**

1. Configure replication slot in PostgreSQL.

    ```yaml
    apiVersion: "acid.zalan.do/v1"
    kind: postgresql
    metadata:
      name: acid-demo-cluster
    spec:
      numberOfInstances: 2
      postgresql:
        parameters:
          wal_level: logical
      patroni:
        slots:
          clickhouse_sync:
            type: logical
            database: demodb
            plugin: pgoutput
    ```

2. Wait for replication slot to be ready, then begin a transaction and export the transaction snapshot identifier:

    ```sql
    BEGIN;
    SELECT pg_export_snapshot();
    ```

3. In ClickHouse create database:

    ```sql
    CREATE DATABASE demodb
    ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
    SETTINGS
      materialized_postgresql_replication_slot = 'clickhouse_sync',
      materialized_postgresql_snapshot = '0000000A-0000023F-3',
      materialized_postgresql_tables_list = 'table1,table2,table3';
    ```

4. End the PostgreSQL transaction once replication to ClickHouse DB is confirmed. Verify that replication continues after failover:

    ```bash
    kubectl exec acid-demo-cluster-0 -c postgres -- su postgres -c 'patronictl failover --candidate acid-demo-cluster-1 --force'
    ```

### Required permissions {#required-permissions}

1. [CREATE PUBLICATION](https://www.postgresql.org/docs/14/sql-createpublication.html) -- create query privilege.

2. [CREATE_REPLICATION_SLOT](https://www.postgresql.org/docs/10/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-SLOT) -- replication privilege.

3. [pg_drop_replication_slot](https://www.postgresql.org/docs/9.5/functions-admin.html#FUNCTIONS-REPLICATION) -- replication privilege or superuser.

4. [DROP PUBLICATION](https://www.postgresql.org/docs/10/sql-droppublication.html) -- owner of publication (`username` in MaterializedPostgreSQL engine itself).

It is possible to avoid executing `2` and `3` commands and having those permissions. Use settings `materialized_postgresql_replication_slot` and `materialized_postgresql_snapshot`. But with much care.

Access to tables:

1. pg_publication

2. pg_replication_slots

3. pg_publication_tables

### Backup and restore {#backup-and-restore}

A `MaterializedPostgreSQL` database can be backed up. The data of every replicated table lives in a nested `ReplacingMergeTree` table, so `BACKUP DATABASE` captures that data by delegating to the nested table.

```sql
BACKUP DATABASE postgres_db TO Disk('backups', 'postgres_db.zip');
```

Restoring a `MaterializedPostgreSQL` database or table **in place is not supported**. A restored `MaterializedPostgreSQL` object immediately starts replicating from the live PostgreSQL source, so restoring the backup snapshot on top of it would mix the snapshot with the current remote state. RESTORE therefore fails closed in this case. Restore the captured data into plain `ReplacingMergeTree` tables instead:

- In a database backup, each table's stored definition is already the synthetic nested `ReplacingMergeTree` (not the `MaterializedPostgreSQL` engine), so each table can be restored straight into a new, not-yet-existing table:

    ```sql
    RESTORE TABLE postgres_db.table1 AS restored_db.table1
    FROM Disk('backups', 'postgres_db.zip')
    SETTINGS allow_different_table_def = 1;
    ```

- For a standalone `MaterializedPostgreSQL` table backup, the stored definition is the `MaterializedPostgreSQL` engine itself. Create a `ReplacingMergeTree` table beforehand with the same structure as the nested table (including the `_sign` and `_version` columns) and restore into it:

    ```sql
    RESTORE TABLE src AS existing_replacing_mergetree
    FROM Disk('backups', 'table.zip')
    SETTINGS allow_different_table_def = 1;
    ```
)DOCS_MD",
        .syntax = "ENGINE = MaterializedPostgreSQL('host:port', 'database', 'user', 'password')",
        .related = {"PostgreSQL"}});
}
}

#endif
