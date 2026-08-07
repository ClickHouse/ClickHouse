#include <Databases/PostgreSQL/DatabaseMaterializedPostgreSQL.h>
#include <Storages/PostgreSQL/MaterializedPostgreSQLSettings.h>

#if USE_LIBPQXX

#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>

#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>
#include <Common/FailPoint.h>
#include <Common/Macros.h>
#include <Common/PoolId.h>
#include <Common/parseAddress.h>
#include <Common/parseRemoteDescription.h>
#include <Common/RemoteHostFilter.h>
#include <Common/AsyncLoader.h>
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
    extern const MaterializedPostgreSQLSettingsString materialized_postgresql_keeper_path;
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
    extern const int FAULT_INJECTED;
    extern const int POSTGRESQL_REPLICATION_INTERNAL_ERROR;
}

namespace FailPoints
{
    extern const char materialized_postgresql_fail_nested_table_drop[];
    extern const char materialized_postgresql_fail_database_startup[];
    extern const char materialized_postgresql_pause_after_stop_replication[];
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

std::shared_ptr<PostgreSQLReplicationHandler> DatabaseMaterializedPostgreSQL::makeReplicationHandler()
{
    return std::make_shared<PostgreSQLReplicationHandler>(
            remote_database_name,
            /* table_name */"",
            TSA_SUPPRESS_WARNING_FOR_READ(database_name),     /// FIXME
            toString(getUUID()),
            connection_info,
            getContext(),
            is_attach,
            *settings,
            /* is_materialized_postgresql_database = */ true);
}


void DatabaseMaterializedPostgreSQL::startSynchronization()
{
    std::lock_guard lock(handler_mutex);
    if (shutdown_called)
        return;

    /// The startup task can be rescheduled after synchronization has already started - e.g. by a refused
    /// (fail-close) DROP DATABASE, which had deactivated it up front and must restore it on failure (see
    /// `beforeDropDatabase`). Replacing a live handler here would leak its running consumer, so make the
    /// task idempotent instead. Failed startups never set the flag: `tryStartSynchronization` keeps
    /// retrying them as before.
    if (synchronization_started)
        return;

    /// Simulates the background startup failing before the replication handler has been built, to make the
    /// attach/restart window - in which `replication_handler` is still null while the database is already
    /// mounted and accepts DDL - deterministically testable.
    fiu_do_on(FailPoints::materialized_postgresql_fail_database_startup,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED,
            "Injected failure of the MaterializedPostgreSQL database startup");
    });

    replication_handler = makeReplicationHandler();

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

        storage->as<StorageMaterializedPostgreSQL>()->setCoordinated(isCoordinated());

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
    synchronization_started = true;
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
        else if (change.name == "materialized_postgresql_table_engine"
                 || change.name == "materialized_postgresql_keeper_path"
                 || change.name == "materialized_postgresql_replica_name")
        {
            throw Exception(ErrorCodes::QUERY_NOT_ALLOWED,
                            "Setting `{}` defines the engine of the nested tables and the coordination identity of this "
                            "replica, and can only be set at CREATE time: the nested tables and the coordination state in "
                            "Keeper are already built from it. Recreate the database to change it.", change.name);
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
            /// The replication handler is built by the background startup task, so right after CREATE / ATTACH
            /// or a server restart it may not exist yet (and it keeps not existing while the startup task is
            /// retrying an unreachable PostgreSQL). The change is still applied to the in-memory `settings` and
            /// persisted to the on-disk metadata below, and `makeReplicationHandler` reads `*settings`, so a
            /// handler built later picks the new value up. A handler that exists but has no consumer yet (a
            /// coordinated standby, or a failed startup being retried) stores the value and passes it to the
            /// consumer it creates later.
            if (replication_handler)
                replication_handler->setSetting(change);
            need_update_on_disk = true;
        }

        settings->applyChange(change);
    }

    if (need_update_on_disk)
        DatabaseOnDisk::modifySettingsMetadata(settings_changes, query_context);
}


bool DatabaseMaterializedPostgreSQL::isCoordinated() const
{
    return !(*settings)[MaterializedPostgreSQLSetting::materialized_postgresql_keeper_path].value.empty();
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

    /// The replication handler is built by the background startup task and may not exist yet in the
    /// attach/restart window (a null dereference below would not be caught by the try/catch).
    if (!replication_handler)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                "Cannot get the definition of table `{}`: the database has not finished starting replication yet. "
                "Retry once synchronization has started", table_name);
        return nullptr;
    }

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

    /// Refuse before creating the nested table below (it would be left behind by a throw later in
    /// attachTable): see PostgreSQLReplicationHandler::addTableToReplication for the reasoning.
    /// Checked on the settings (not on the handler, which is only created by the background startup task).
    if (!(*settings)[MaterializedPostgreSQLSetting::materialized_postgresql_keeper_path].value.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "ATTACH TABLE is not supported for a coordinated MaterializedPostgreSQL setup "
            "(materialized_postgresql_keeper_path is set). "
            "Recreate the database with an updated materialized_postgresql_tables_list instead");

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
        /// Refuse before mutating anything (the tables-list setting is altered below and would not be
        /// rolled back): see PostgreSQLReplicationHandler::addTableToReplication for the reasoning.
        /// Checked on the settings (not on the handler, which is only created by the background startup task).
        if (!(*settings)[MaterializedPostgreSQLSetting::materialized_postgresql_keeper_path].value.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "ATTACH TABLE is not supported for a coordinated MaterializedPostgreSQL setup "
                "(materialized_postgresql_keeper_path is set). "
                "Recreate the database with an updated materialized_postgresql_tables_list instead");

        /// The replication handler is built by the background startup task, so right after CREATE / ATTACH or a
        /// server restart it may not exist yet (and it keeps not existing while the startup task is retrying an
        /// unreachable PostgreSQL). Adding a table requires the live handler, so refuse cleanly - and do it
        /// before mutating anything: the tables-list setting altered below would not be rolled back.
        {
            std::lock_guard lock(handler_mutex);
            if (!replication_handler)
                throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                    "Cannot add table `{}` to replication: the database has not finished starting replication yet. "
                    "Retry once synchronization has started", table_name);
        }

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
            if (!replication_handler)
                throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                    "Cannot add table `{}` to replication: the replication handler is gone. Retry later", table_name);
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
        /// Refuse before mutating anything (the tables-list setting is altered below and would not be
        /// rolled back): see PostgreSQLReplicationHandler::removeTableFromReplication for the reasoning.
        /// Checked on the settings (not on the handler, which is only created by the background startup task).
        if (!(*settings)[MaterializedPostgreSQLSetting::materialized_postgresql_keeper_path].value.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "DETACH TABLE PERMANENTLY is not supported for a coordinated MaterializedPostgreSQL setup "
                "(materialized_postgresql_keeper_path is set). "
                "Recreate the database with an updated materialized_postgresql_tables_list instead");

        /// Same startup-window handling as in `attachTable`: removing a table requires the live replication
        /// handler, which the background startup task may not have built yet. Refuse cleanly before mutating
        /// anything (the tables-list setting altered below would not be rolled back).
        {
            std::lock_guard lock(handler_mutex);
            if (!replication_handler)
                throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                    "Cannot remove table `{}` from replication: the database has not finished starting replication yet. "
                    "Retry once synchronization has started", table_name);
        }

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
        if (!replication_handler)
            throw Exception(ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR,
                "Cannot remove table `{}` from replication: the replication handler is gone. Retry later", table_name);
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

    {
        std::lock_guard lock(handler_mutex);
        if (replication_handler)
            replication_handler->shutdown();
        synchronization_started = false;
    }

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

    /// Holds the drop path inside the window of DROP DATABASE between stopping replication and shutting the
    /// database down (see `InterpreterDropQuery::executeToDatabaseImpl`), in which a still-armed background
    /// startup retry could restart replication and recreate the PostgreSQL publication and slot while the drop
    /// is in flight - `beforeDropDatabase` deactivates the startup task to make that impossible. Paused outside
    /// `handler_mutex` so such a retry could actually run if it were still armed.
    fiu_do_on(FailPoints::materialized_postgresql_pause_after_stop_replication,
    {
        LOG_INFO(log, "Pausing after stopping replication until failpoint "
                 "materialized_postgresql_pause_after_stop_replication is disabled");
        FailPointInjection::pauseFailPoint(FailPoints::materialized_postgresql_pause_after_stop_replication);
    });
}


void DatabaseMaterializedPostgreSQL::dropTable(ContextPtr local_context, const String & table_name, bool sync)
{
    /// Defense-in-depth for the coordinated-mode guard in `StorageMaterializedPostgreSQL::checkTableCanBeDropped`:
    /// a user-issued `DROP TABLE` of an individual table would remove the local nested replicated table
    /// without updating the shared publication, silently diverging replicas. The storage-level check already
    /// rejects it before `flushAndShutdown`; reject here too in case the storage check is ever bypassed.
    /// DROP DATABASE and internal cleanup run with an internal context and must still be able to drop the
    /// nested tables, so only a genuine (non-internal) user query is refused.
    if (isCoordinated() && !local_context->isInternalQuery())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "DROP TABLE is not supported for a coordinated MaterializedPostgreSQL setup "
            "(materialized_postgresql_keeper_path is set). "
            "Recreate the database with an updated materialized_postgresql_tables_list instead");

    /// Simulates the nested-table drop of a DROP DATABASE failing (e.g. Keeper disappearing while a nested
    /// ReplicatedReplacingMergeTree deletes its own Keeper metadata, or a filesystem error for a plain nested
    /// table) after `beforeDropDatabase` has already deactivated the startup task, to test recovery via
    /// `onDropDatabaseFailed` in both modes.
    if (local_context->isInternalQuery())
        fiu_do_on(FailPoints::materialized_postgresql_fail_nested_table_drop,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED,
                "Injected failure while dropping a nested table of a coordinated MaterializedPostgreSQL database");
        });

    FailPointInjection::pauseFailPoint(FailPoints::database_materialized_postgresql_pause_before_table_drop);

    /// Modify context into nested_context and pass query to Atomic database.
    /// In coordinated mode drop the nested replicated table without the usual
    /// `database_atomic_delay_before_drop_table_sec` delay. The DROP DATABASE teardown removes this replica's
    /// registration (and, for the last replica, the shared coordination nodes) synchronously, so leaving the
    /// nested tables' Keeper trees behind for the delay window would let a prompt CREATE on the same keeper
    /// path adopt a half-dead shared tree: a ghost replica that never answers part fetches, and stale block
    /// deduplication hashes.
    DatabaseAtomic::dropTable(StorageMaterializedPostgreSQL::makeNestedTableContext(local_context), table_name, isCoordinated() || sync);
}


void DatabaseMaterializedPostgreSQL::renameTable(
    ContextPtr local_context, const String & table_name, IDatabase & to_database,
    const String & to_table_name, bool exchange, bool dictionary)
{
    /// Reject RENAME / EXCHANGE TABLE. The base `DatabaseAtomic::renameTable` would rename only the local nested
    /// table, while the replication state keeps the PostgreSQL table name: the cached `materialized_tables`
    /// wrappers (so `tryGetTable` still serves the old key), the handler's `materialized_storages`, the persisted
    /// `materialized_postgresql_tables_list` and - in coordinated mode - the shared publication and every peer
    /// replica. `SHOW TABLES` would follow the renamed nested metadata while reads and the replication startup
    /// still look for the original name, and a coordinated setup would silently diverge from its peers. Renaming
    /// a replicated table is not meaningful anyway: the name mirrors the PostgreSQL table it replicates. Only
    /// genuine (non-internal) user queries are refused; internal cleanup must still be able to move nested tables.
    if (!local_context->isInternalQuery())
    {
        if (isCoordinated())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "RENAME / EXCHANGE TABLE is not supported for a coordinated MaterializedPostgreSQL setup "
                "(materialized_postgresql_keeper_path is set). "
                "Recreate the database with an updated materialized_postgresql_tables_list instead");

        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "RENAME / EXCHANGE TABLE is not supported for a MaterializedPostgreSQL database: the name of a "
            "replicated table mirrors the name of the PostgreSQL table it replicates");
    }

    DatabaseAtomic::renameTable(local_context, table_name, to_database, to_table_name, exchange, dictionary);
}


void DatabaseMaterializedPostgreSQL::beforeTruncateDatabase(ContextPtr local_context)
{
    /// Reject a database-wide TRUNCATE. Both `TRUNCATE DATABASE db` (which drops each nested table) and
    /// `TRUNCATE ALL TABLES FROM db` (which truncates each one) are executed by walking the nested tables through
    /// an internal context (see `InterpreterDropQuery::executeToDatabaseImpl`, which also deliberately skips
    /// `stopReplication` for a truncate), so they operate on the local nested storages directly and never reach
    /// the per-table `StorageMaterializedPostgreSQL::checkTableCanBeDropped` guard. The replication handler stays
    /// live with its slot and publication, so the local copy is wiped while replication keeps advancing from the
    /// current `confirmed_flush_lsn`: the truncated rows are never reloaded and the database stops reflecting
    /// PostgreSQL. In coordinated mode this additionally wipes one replica's copy of the shared replicated data
    /// while the shared slot/publication/`snapshot_completed` marker survive. There is no stop/drop/resnapshot
    /// path behind a truncate, so refuse it up front. Only genuine (non-internal) user queries are refused;
    /// internal cleanup must still be able to remove nested tables.
    if (!local_context->isInternalQuery())
    {
        if (isCoordinated())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "TRUNCATE DATABASE / TRUNCATE ALL TABLES is not supported for a coordinated MaterializedPostgreSQL "
                "setup (materialized_postgresql_keeper_path is set). "
                "Recreate the database with an updated materialized_postgresql_tables_list instead");

        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "TRUNCATE DATABASE / TRUNCATE ALL TABLES is not supported for a MaterializedPostgreSQL database: "
            "it would delete the local copy of the replicated data while replication continues from the current "
            "position in PostgreSQL, so the deleted rows would never be reloaded. "
            "Recreate the database to reload it from a fresh snapshot instead");
    }
}


void DatabaseMaterializedPostgreSQL::beforeDropDatabase(ContextPtr)
{
    /// The generic DROP DATABASE path drops every nested table (in `InterpreterDropQuery::executeToDatabaseImpl`)
    /// before it ever reaches `DatabaseMaterializedPostgreSQL::drop` / `PostgreSQLReplicationHandler::
    /// shutdownFinal`. In coordinated mode the nested tables are this replica's local copy of the shared
    /// replicated data, so the last-replica teardown of the shared slot/publication/marker has to be decided
    /// here - while the nested tables still exist - not after they are gone.
    ///
    /// Stop the background startup task first, in every mode (outside `handler_mutex`, which
    /// `startSynchronization` also takes). The generic drop path calls `stopReplication` right after this hook,
    /// which clears `synchronization_started` without quiescing the task - so a still-armed startup retry waking
    /// mid-drop could re-enter `startSynchronization` and recreate the PostgreSQL publication and replication
    /// slot (and even nested tables) while the drop is already tearing the database down. In coordinated mode it
    /// additionally must not build the handler / create the nested tables concurrently with the teardown below.
    /// A refused drop re-arms the task in `onDropDatabaseFailed` (or in the catch below), so deactivating it
    /// never leaves a database whose background startup had not run yet (attach/restart window) mounted but
    /// permanently not synchronizing.
    startup_task->deactivate();

    /// Only coordinated databases have teardown work to do here.
    if (!isCoordinated())
        return;

    std::lock_guard lock(handler_mutex);

    /// `coordinatedTeardownBeforeDataDrop` makes the last-replica decision fail-close: if Keeper is unreachable
    /// it throws, aborting the drop before any nested table is removed (retry once Keeper is reachable again); if
    /// this is the last replica it removes the shared coordination nodes now so a Keeper outage or a failure
    /// during the subsequent nested-table drop can never leave the shared state behind after the last copy is
    /// deleted; and if it is not the last replica it keeps this replica registered until `drop` removes it, after
    /// the nested tables have actually been dropped.
    try
    {
        /// `startSynchronization` may never have run: on attach or after a restart the coordinated nested tables
        /// and the persistent `<keeper_path>/replicas/<name>` registration already exist, but `replication_handler`
        /// is still null until the background startup task builds it. Build it from the persisted settings now,
        /// purely to run the fail-close teardown below (constructing the handler only resolves the coordination
        /// path / replica-name macros; it does not connect to PostgreSQL or start replication). Without this,
        /// dropping the database in that window would delete the local nested tables without unregistering this
        /// replica or removing the shared slot/publication/`snapshot_completed` marker, and a later recreate on
        /// the same keeper path could resume from `confirmed_flush_lsn` into empty tables.
        if (!replication_handler)
            replication_handler = makeReplicationHandler();

        replication_handler->coordinatedTeardownBeforeDataDrop();
    }
    catch (...)
    {
        /// The drop is refused and the database stays alive, so recover: undo the `deactivate` above and, if the
        /// teardown had already stopped the live handler, discard it so the startup task rebuilds replication.
        recoverAfterRefusedDrop();
        throw;
    }
}


void DatabaseMaterializedPostgreSQL::recoverAfterRefusedDrop()
{
    /// Undo the `deactivate` from `beforeDropDatabase`: a database whose background startup had not run yet
    /// (attach/restart window) must still be able to build its handler and start (or, in coordinated mode,
    /// rejoin) replication; without this it would stay mounted but dead until a server restart. For a database
    /// whose synchronization already started the rescheduled task is a no-op (see the `synchronization_started`
    /// guard in `startSynchronization`).
    ///
    /// If the handler had already been stopped when the drop was refused - by `stopReplication` in the generic
    /// drop path, or by the coordinated teardown's one post-shutdown step (the last replica's removal of the
    /// shared coordination nodes) - re-arming the startup task alone would not recover while
    /// `synchronization_started` is still set: the flag would make the restarted task a no-op with the handler
    /// staying dead. Discard the stopped handler and clear the flag, so the startup task rebuilds replication
    /// from scratch.
    if (replication_handler && replication_handler->isStopped())
    {
        replication_handler.reset();
        synchronization_started = false;
    }
    if (!shutdown_called)
        startup_task->activateAndSchedule();
}


void DatabaseMaterializedPostgreSQL::onDropDatabaseFailed(ContextPtr)
{
    /// Reached when a DROP DATABASE was refused (threw) after `beforeDropDatabase` returned successfully - most
    /// importantly when the generic drop path then failed to remove one of the nested tables (e.g. Keeper
    /// disappeared while a nested ReplicatedReplacingMergeTree was deleting its own Keeper metadata). By that
    /// point `beforeDropDatabase` has already deactivated the startup task and the generic drop path has already
    /// run `stopReplication`, so without this the database would stay mounted but permanently not consuming until
    /// a server restart, which violates the contract that a refused drop never leaves the database silently dead.
    /// This applies to both modes: the coordinated teardown ran in `beforeDropDatabase`, but the startup-task
    /// re-arm is needed for a plain database just as much. Recovery is idempotent, so a double call (a failure
    /// inside `beforeDropDatabase`, which recovers in its own catch, is also routed here) is harmless.
    std::lock_guard lock(handler_mutex);
    recoverAfterRefusedDrop();
}


void DatabaseMaterializedPostgreSQL::drop(ContextPtr local_context)
{
    /// Reached after the generic DROP DATABASE path has already dropped the nested tables. In coordinated mode
    /// the last-replica decision (and, for the last replica, the removal of the shared coordination nodes) has
    /// already been made in `beforeDropDatabase`, while the nested tables still existed. `shutdownFinal` here is
    /// the authoritative post-data teardown: it removes this replica's registration for the non-last case (now
    /// that its nested tables are gone) and cleans up the shared PostgreSQL slot/publication for the last case;
    /// running it after `beforeDropDatabase` is idempotent. In non-coordinated mode this is the only teardown.
    std::lock_guard lock(handler_mutex);

    /// `replication_handler` may still be null here if the drop happened before the background startup task ever
    /// ran (attach/restart window; in coordinated mode `beforeDropDatabase` has already built it). The PostgreSQL
    /// publication and the logical replication slot exist independently of the handler object, so skipping the
    /// teardown would leak them in PostgreSQL after the database is gone (a leaked slot retains WAL and can
    /// exhaust `max_replication_slots`). Build the handler from the persisted settings, purely to run the final
    /// cleanup (constructing it does not connect to PostgreSQL or start replication).
    if (!replication_handler)
        replication_handler = makeReplicationHandler();
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

        if (!args.create_query.attach)
        {
            /// `{uuid}` in the coordination path is only safe when every replica ends up with the same
            /// UUID, which is the case exactly when the DDL carries it: an ON CLUSTER query, or an
            /// explicit `UUID '...'` clause. Otherwise each server generates its own UUID. This mirrors
            /// how `TableZnodeInfo::resolve` decides whether a ReplicatedMergeTree path may use `{uuid}`.
            const bool allow_uuid_macro = args.context->isDDLOrOnClusterInternal() || args.create_query.has_uuid;
            validateMaterializedPostgreSQLCoordinationSettings(
                *postgresql_replica_settings, args.context, args.database_name, args.uuid,
                configuration.database, /* postgres_table */ "", allow_uuid_macro);
        }

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

`RENAME TABLE` and `EXCHANGE TABLES` are not supported for a `MaterializedPostgreSQL` database: the name of a replicated table mirrors the name of the PostgreSQL table it replicates, and renaming only the local table would leave the replication state (including the persisted [`materialized_postgresql_tables_list`](#materialized-postgresql-tables-list)) pointing at the original name. A database-wide truncate (`TRUNCATE DATABASE` / `TRUNCATE ALL TABLES FROM`) is not supported either: it would delete the local copy of the replicated data while replication continues from the current position in PostgreSQL, so the deleted rows would never be reloaded. Recreate the database to reload it from a fresh snapshot instead.

In coordinated mode (see [`materialized_postgresql_keeper_path`](#materialized-postgresql-keeper-path)) adding and removing individual tables is not supported at all, because it would change only the local replica; recreate the database with an updated table list instead.

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

In coordinated mode the query is also accepted on a standby replica (which has no active consumer) - including a former active worker that has been demoted back to standby: the new value is applied when that replica later becomes the active worker.

### `materialized_postgresql_use_unique_replication_consumer_identifier` {#materialized_postgresql_use_unique_replication_consumer_identifier}

Use a unique replication consumer identifier for replication. Default: `0`.
If set to `1`, allows to setup several `MaterializedPostgreSQL` tables pointing to the same `PostgreSQL` table.

### `materialized_postgresql_use_extended_date_and_time_types` {#materialized-postgresql-use-extended-date-and-time-types}

Map the PostgreSQL `date` and `timestamp`/`timestamptz` types to ClickHouse `Date32` and `DateTime64`, which cover the wider value range of the PostgreSQL types. Default: `1`.
If set to `0`, the narrower `Date` and `DateTime` types are used instead (values outside their range or with sub-second precision are not representable).

This setting only controls the column types chosen by type inference when the nested tables are created, so it must be specified at `CREATE DATABASE` time. It cannot be changed afterwards with `ALTER DATABASE ... MODIFY SETTING` (the already created nested tables keep their fixed column types, and such a change is rejected); recreate the database to change it. It is not applicable to the `MaterializedPostgreSQL` table engine, where the column types are declared explicitly.

### `materialized_postgresql_table_engine` {#materialized-postgresql-table-engine}

Engine used for the nested tables that the engine creates. One of `ReplacingMergeTree` (default), `ReplicatedReplacingMergeTree`, `SharedReplacingMergeTree`. The replicated and shared variants require [`materialized_postgresql_keeper_path`](#materialized-postgresql-keeper-path) to be set, which enables cross-replica coordination of the replication slot. `SharedReplacingMergeTree` is only available in ClickHouse Cloud.

It must be specified at `CREATE` time (it determines how the nested tables are created and cannot be changed afterwards).

### `materialized_postgresql_keeper_path` {#materialized-postgresql-keeper-path}

Keeper (or ZooKeeper) path used to coordinate the PostgreSQL logical replication slot across ClickHouse replicas. Default: empty (coordination disabled).

When set, coordination is enabled: exactly one replica (the "active worker") consumes the replication slot at a time, and the others stand by and take over automatically when it becomes unavailable. This is what makes it safe to use a replicated/shared nested table engine for high availability - a PostgreSQL logical replication slot allows only one active consumer, so without coordination two replicas would race and lose changes. The active worker is elected with an ephemeral node in Keeper (similar to `S3Queue`, the Keeper-coordinated `Kafka` engine, and refreshable materialized views), so failover happens once the previous worker's Keeper session ends.

The path supports the `{uuid}` and `{shard}` macros. It **must resolve to the same value on every participating replica** (it is both the coordination namespace and the root of the shared nested tables), so a per-replica or per-server macro such as `{replica}` or `{server_uuid}` is **rejected at `CREATE` time** - including when it is reached indirectly through a config macro that expands to one - because it would place each replica on a disjoint Keeper subtree. Put the per-replica part in [`materialized_postgresql_replica_name`](#materialized-postgresql-replica-name) instead. A misspelled or unsupported macro in this path or in `materialized_postgresql_replica_name` is also rejected at `CREATE` time (both settings are macro-expanded during validation exactly as the replication handler expands them later), instead of surfacing only in the background startup task. The `{uuid}` macro is accepted only when the UUID is guaranteed to be identical on every replica - an `ON CLUSTER` query, a table inside a `Replicated` database, or an explicit `UUID '...'` clause in the `CREATE` query. A plain `CREATE` generates its own UUID on every server, so `{uuid}` is **rejected at `CREATE` time** there: the replicas would not only sit on disjoint Keeper subtrees, they would still contend for the same PostgreSQL replication slot and publication (their names are derived from the PostgreSQL source, not from the keeper path), each believing it is the only active worker, and WAL could be lost. It cannot be combined with [`materialized_postgresql_use_unique_replication_consumer_identifier`](#materialized_postgresql_use_unique_replication_consumer_identifier), because coordination relies on a single shared replication slot. For the same reason it cannot be combined with a user-managed [`materialized_postgresql_replication_slot`](#materialized-postgresql-replication-slot) or [`materialized_postgresql_snapshot`](#materialized-postgresql-snapshot): coordination owns the shared slot and must be able to drop and recreate it (re-exporting a fresh snapshot) if the active worker dies before the initial snapshot completes, which is impossible for a slot it does not manage. It also requires [`materialized_postgresql_table_engine`](#materialized-postgresql-table-engine) to be set to `ReplicatedReplacingMergeTree` or `SharedReplacingMergeTree`: with a plain `ReplacingMergeTree` the standby replicas would hold no data, so a takeover would lose every row replicated before the failover. Coordination stores its leader/replica/snapshot nodes in Keeper and the nested tables are replicated, so Keeper (or ZooKeeper) must be configured on the server; a coordinated `CREATE DATABASE` on a server with no Keeper is rejected at `CREATE` time rather than left retrying in the background.

Only the active worker loads the initial snapshot; the other replicas receive the data (both the snapshot and ongoing changes) through ClickHouse replication of the shared replicated nested tables. A durable snapshot marker in Keeper records that the initial snapshot loaded every table: a new active worker resumes from the slot's confirmed position only when the marker exists, and otherwise clears the nested tables and redoes the snapshot from scratch. The marker is fenced on the live leadership session - a worker whose Keeper session expires mid-snapshot aborts instead of publishing the marker or starting a consumer, so a deposed worker can never mask its successor's replacement snapshot with a stale marker. The redo of the snapshot is fenced the same way: a worker whose leadership session is no longer alive aborts before truncating the nested tables and before dropping or recreating the shared slot, so a deposed worker cannot wipe the tables its successor has already reloaded or discard the slot the successor just created. An active worker whose startup fails before its consumer is running (for example, the snapshot of any of the tables cannot be loaded) aborts the whole attempt - it never starts a consumer that would advance the shared replication slot while skipping the tables whose snapshot failed - and releases the leadership instead of retrying while holding it, so a healthy replica can take over and redo the full snapshot. The replication slot and the PostgreSQL publication are shared by all participating replicas: dropping the database on one replica keeps them for the others and only dropping the last replica removes them from PostgreSQL. That last-replica decision is made in Keeper *before* any nested table is dropped: if this is the last replica, the shared coordination nodes (including the snapshot marker) are removed first, so that even if the subsequent nested-table drop (or the process) fails, the shared state is never left behind after the last copy is deleted; and a `DROP DATABASE` while Keeper is unreachable fails instead of removing the local nested tables - otherwise it could delete the last copy of the data while leaving the shared slot and snapshot marker behind (a later recreate would then resume into empty tables); retry the drop once Keeper is reachable. This holds even for a `DROP DATABASE` issued immediately after a restart, before the background replication has finished starting up (and a `DROP DATABASE` refused in that window leaves the background startup intact, so the replica still rejoins the setup). More generally, a refused (failed) drop never leaves the replica silently dead: if the failure happened only after the drop had already stopped the replica's consumer - including a failure while deleting this replica's own local nested tables (for example if Keeper disappears while a nested replicated table is removing its own Keeper metadata) - replication is rebuilt in the background and the replica rejoins the setup once Keeper is reachable again. The last-replica decision is fenced in Keeper (on the shared `replicas` node) so that even with concurrent `DROP DATABASE` on different replicas only one of them can ever act as the last replica. The last replica's teardown also holds an ownership token under the keeper path from the moment it wins that fence until the shared PostgreSQL slot and publication have actually been dropped; while the token is held, a fresh coordinated `CREATE DATABASE` on the same keeper path is rejected - the token's absence is asserted atomically with the joining replica's registration in Keeper, so even a `CREATE` racing the very start of a teardown cannot slip past the fence - and the pending by-name drops can never delete a new setup's slot or publication. If the tearing-down server dies before completing the teardown, the leftover `teardown` node under the keeper path must be removed manually (after dropping the leftover replication slot and publication in PostgreSQL) before the path can be reused. The nested tables of a coordinated database are dropped without the usual delayed-drop window (as with `DROP TABLE ... SYNC`), so their shared Keeper subtrees do not outlive the `DROP DATABASE`; and if the final PostgreSQL cleanup of the last replica still fails, a leftover publication with no surviving coordination state in Keeper is detected on the next coordinated `CREATE DATABASE` and dropped rather than silently adopted with its stale table set. All replicas of one coordinated setup must also agree on the naming-affecting settings - [`materialized_postgresql_table_engine`](#materialized-postgresql-table-engine), [`materialized_postgresql_schema`](#materialized-postgresql-schema), [`materialized_postgresql_schema_list`](#materialized-postgresql-schema-list) and `materialized_postgresql_tables_list_with_schema` - and must replicate the same PostgreSQL source: the same source database and, for the single-table `MaterializedPostgreSQL` table engine, the same source table (in particular, a coordinated single-table engine and a coordinated database engine can never share one keeper path, because they derive different replication slot and publication names even for the same source table). These determine how the ClickHouse names of the shared nested tables (and the names of the shared replication slot and publication) are derived. The first replica publishes this identity under the keeper path, and a replica that disagrees with it is rejected (at `CREATE` time when the setup already exists in Keeper): it would either adopt the same publication yet build a disjoint replicated tree that never receives the other replicas' data, or share the coordination bookkeeping while working against a different PostgreSQL slot and publication, so that dropping one setup could tear down or leak the other's PostgreSQL objects. The set of replicated tables is fenced the same way: the first replica publishes its derived table set under the keeper path before it builds any nested table (the shared publication is only created later, by the elected active worker), and a replica whose derived set differs is refused - so two replicas created concurrently with different `materialized_postgresql_tables_list` values (or with the same empty value around a source schema change) cannot silently build diverging nested tables on one keeper path. Once the shared publication exists, joining replicas derive their table set from it, so a refused join converges by itself when the lists are reconciled. If the shared publication is temporarily absent - for example it was dropped externally and has to be recreated - the table set fenced in Keeper is used instead of the local `materialized_postgresql_tables_list`, so a replica that once adopted the publication's table set over a mismatching local list keeps that set across restarts and recreates the publication with it. The shared nested-table schema is authoritative: a replica whose PostgreSQL schema has drifted from it (e.g. the source table was altered or renamed after the coordinated database was first created, since `MaterializedPostgreSQL` continues by column position and does not track PostgreSQL DDL) cannot join and reports a schema-drift error; reconcile the PostgreSQL schema or recreate the database. Dynamically adding, removing, or renaming individual tables (`ATTACH TABLE` / `DETACH TABLE PERMANENTLY` / `DROP TABLE` / `RENAME TABLE` / `EXCHANGE TABLES`) is not supported in coordinated mode: each would only change the local replica while the shared publication, tables-list setting and peer replicas keep the old set, silently diverging the replicas. Recreate the database with an updated `materialized_postgresql_tables_list` instead. For the same reason a database-wide truncate (`TRUNCATE DATABASE` / `TRUNCATE ALL TABLES FROM`) is also rejected: it would locally wipe this replica's copy of the shared replicated data while the shared slot, publication and snapshot marker (and the live consumer) stay in place.

Coordinated mode also does not support a column-filtered `materialized_postgresql_tables_list` (e.g. `table1(col1, col2)`): all replicas share one set of nested tables on the same Keeper path and must agree on the exact column projection, but the per-table column list is taken from each replica's local setting rather than from the shared publication, so a column filter is rejected at `CREATE` time. List the tables without column filters so every replica builds the identical shared schema.

### `materialized_postgresql_replica_name` {#materialized-postgresql-replica-name}

Replica identity used for the coordination node and for the nested replicated table engine. Default: `{replica}`. Supports the `{uuid}`, `{shard}` and `{replica}` macros. It **must resolve to a distinct value on every replica**, and this is enforced: each replica's registration node stores its identity, so a `CREATE` whose replica name is already registered by another replica is rejected (synchronously when the registration is already visible in Keeper) instead of silently collapsing two replicas onto one registration, which would corrupt the bookkeeping that decides when the last replica removes the shared replication slot and publication.

The expanded value must also be a **single Keeper node name**: an empty value, or a value containing `/` (such as `'{shard}/{replica}'`), is rejected at `CREATE` time and again when the replica registers itself. Replicas are tracked as `<keeper_path>/replicas/<name>`, and the last-replica fence fires by removing the `/replicas` node once it becomes empty; with an extra path level in between it never becomes empty, so the shared replication slot, publication and snapshot marker would leak forever, even after the last replica is dropped.

Together with [`materialized_postgresql_keeper_path`](#materialized-postgresql-keeper-path), this setting forms the **coordination identity** of the replica, which must stay the same for the lifetime of the coordinated setup. Both settings are re-expanded from the current server configuration on every startup, while the shared nested tables keep the expansion they were created with and the `<keeper_path>/replicas/<name>` registration is persistent. A configuration-only change of a macro they expand through (directly, or through an intermediate config macro) is therefore refused when the replica starts up, with an error naming both the new and the previously used identity: continuing would make the replica elect, register and tear down under a different Keeper identity than the shared data it already holds, leaving the old `/replicas` subtree unable to drain (leaking the shared replication slot, publication and snapshot marker) and possibly splitting leader election from the shared nested-table path. Restore the configuration to the values the replica was created with, or drop the engine on that replica and recreate it on the new coordination path.

The refusal keeps the engine mounted and droppable, and dropping it in that state tears down the coordination state that actually exists: the drop path takes the identity persisted in the metadata of the nested tables the replica owns, not the one the current configuration expands to, so it unregisters and makes its last-replica decision under the original identity instead of orphaning it. This also holds when the settings cannot be expanded at all any more, for instance because a macro they go through was removed from the configuration.

## Notes {#notes}

### Failover of the logical replication slot {#logical-replication-slot-failover}

Logical Replication Slots which exist on the primary are not available on standby replicas.
So if there is a failover, new primary (the old physical standby) won't be aware of any slots which were existing with old primary. This will lead to a broken replication from PostgreSQL.
A solution to this is to manage replication slots yourself and define a permanent replication slot (some information can be found [here](https://patroni.readthedocs.io/en/latest/SETTINGS.html)). You'll need to pass slot name via `materialized_postgresql_replication_slot` setting, and it has to be exported with `EXPORT SNAPSHOT` option. The snapshot identifier needs to be passed via `materialized_postgresql_snapshot` setting.

Please note that this should be used only if it is actually needed. If there is no real need for that or full understanding why, then it is better to allow the table engine to create and manage its own replication slot.

Alternatively, for high availability across ClickHouse replicas, set [`materialized_postgresql_keeper_path`](#materialized-postgresql-keeper-path) together with a replicated nested table engine ([`materialized_postgresql_table_engine`](#materialized-postgresql-table-engine)). Several ClickHouse replicas then share one replication slot: exactly one of them consumes it at a time and the others take over automatically on failure, while the nested tables are kept in sync as ClickHouse replicas.

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
