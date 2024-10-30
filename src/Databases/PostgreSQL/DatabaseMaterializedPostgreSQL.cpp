#include <Databases/PostgreSQL/DatabaseMaterializedPostgreSQL.h>

#if USE_LIBPQXX

#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>

#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <Common/Macros.h>
#include <Common/PoolId.h>
#include <Common/parseAddress.h>
#include <Common/parseRemoteDescription.h>
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
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Common/escapeForFileName.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 postgresql_connection_attempt_timeout;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_NOT_ALLOWED;
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
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
    , startup_task(getContext()->getSchedulePool().createTask("MaterializedPostgreSQLDatabaseStartup", [this]{ tryStartSynchronization(); }))
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
        materialized_tables[table_name] = storage;

        /// Let replication thread know, which tables it needs to keep in sync.
        replication_handler->addStorage(table_name, storage->as<StorageMaterializedPostgreSQL>());
    }

    LOG_TRACE(log, "Loaded {} tables. Starting synchronization", materialized_tables.size());

    replication_handler->startup(/* delayed */false);
}


LoadTaskPtr DatabaseMaterializedPostgreSQL::startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode)
{
    auto base = DatabaseAtomic::startupDatabaseAsync(async_loader, std::move(startup_after), mode);
    auto job = makeLoadJob(
        base->goals(),
        TablesLoaderBackgroundStartupPoolId,
        fmt::format("startup MaterializedMySQL database {}", getDatabaseName()),
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
    bool need_update_on_disk = false;

    for (const auto & change : settings_changes)
    {
        if (!settings->has(change.name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine {} does not support setting `{}`", getEngineName(), change.name);

        if ((change.name == "materialized_postgresql_tables_list"))
        {
            if (!query_context->isInternalQuery())
                throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Changing setting `{}` is not allowed", change.name);

            need_update_on_disk = true;
        }
        else if ((change.name == "materialized_postgresql_allow_automatic_update") || (change.name == "materialized_postgresql_max_block_size"))
        {
            replication_handler->setSetting(change);
            need_update_on_disk = true;
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting");
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
    /// Also if materialized_tables set is empty - it means all access is done to ReplacingMergeTree tables - it is a case after
    /// replication_handler was shutdown.
    if (local_context->isInternalQuery() || materialized_tables.empty())
    {
        return DatabaseAtomic::tryGetTable(name, local_context);
    }

    /// Note: In select query we call MaterializedPostgreSQL table and it calls tryGetTable from its nested.
    /// So the only point, where synchronization is needed - access to MaterializedPostgreSQL table wrapper over nested table.
    std::lock_guard lock(tables_mutex);
    auto table = materialized_tables.find(name);

    /// Return wrapper over ReplacingMergeTree table. If table synchronization just started, table will not
    /// be accessible immediately. Table is considered to exist once its nested table was created.
    if (table != materialized_tables.end() && table->second->as <StorageMaterializedPostgreSQL>()->hasNested())
    {
        return table->second;
    }

    return StoragePtr{};
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

    std::lock_guard lock(handler_mutex);

    ASTPtr ast_storage;
    try
    {
        auto storage = std::make_shared<StorageMaterializedPostgreSQL>(StorageID(TSA_SUPPRESS_WARNING_FOR_READ(database_name), table_name), getContext(), remote_database_name, table_name);
        ast_storage = replication_handler->getCreateNestedTableQuery(storage.get(), table_name);
        assert_cast<ASTCreateQuery *>(ast_storage.get())->uuid = UUIDHelpers::generateV4();
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
    auto set = std::make_shared<ASTSetQuery>();
    set->is_standalone = false;
    set->changes = {new_setting};

    auto command = std::make_shared<ASTAlterCommand>();
    command->type = ASTAlterCommand::Type::MODIFY_DATABASE_SETTING;
    command->settings_changes = command->children.emplace_back(std::move(set)).get();

    auto command_list = std::make_shared<ASTExpressionList>();
    command_list->children.push_back(command);

    auto query = std::make_shared<ASTAlterQuery>();
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
    if (CurrentThread::isInitialized() && CurrentThread::get().getQueryContext())
    {
        auto current_context = Context::createCopy(getContext()->getGlobalContext());
        current_context->setInternalQuery(true);

        /// We just came from createTable() and created nested table there. Add assert.
        auto nested_table = DatabaseAtomic::tryGetTable(table_name, current_context);
        assert(nested_table != nullptr);

        try
        {
            auto tables_to_replicate = settings->materialized_postgresql_tables_list.value;
            if (tables_to_replicate.empty())
                tables_to_replicate = getFormattedTablesList();

            /// tables_to_replicate can be empty if postgres database had no tables when this database was created.
            SettingChange new_setting("materialized_postgresql_tables_list", tables_to_replicate.empty() ? table_name : (tables_to_replicate + "," + table_name));
            auto alter_query = createAlterSettingsQuery(new_setting);

            InterpreterAlterQuery(alter_query, current_context).execute();

            auto storage = std::make_shared<StorageMaterializedPostgreSQL>(table, getContext(), remote_database_name, table_name);
            materialized_tables[table_name] = storage;

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
    if (CurrentThread::isInitialized() && CurrentThread::get().getQueryContext())
    {
        auto & table_to_delete = materialized_tables[table_name];
        if (!table_to_delete)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Materialized table `{}` does not exist", table_name);

        auto tables_to_replicate = getFormattedTablesList(table_name);

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
            materialized_tables.erase(table_name);

            e.addMessage("while removing table `" + table_name + "` from replication");
            throw;
        }

        materialized_tables.erase(table_name);
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
    std::lock_guard lock(handler_mutex);
    if (replication_handler)
        replication_handler->shutdown();

    /// Clear wrappers over nested, all access is not done to nested tables directly.
    materialized_tables.clear();
}


void DatabaseMaterializedPostgreSQL::dropTable(ContextPtr local_context, const String & table_name, bool sync)
{
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
    /// Modify context into nested_context and pass query to Atomic database.
    return DatabaseAtomic::getTablesIterator(StorageMaterializedPostgreSQL::makeNestedTableContext(local_context), filter_by_table_name, skip_not_loaded);
}

void registerDatabaseMaterializedPostgreSQL(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;
        ASTs & engine_args = engine->arguments->children;
        const String & engine_name = engine_define->engine->name;

        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);

        StoragePostgreSQL::Configuration configuration;

        if (!engine->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", engine_name);

        if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, args.context))
        {
            configuration = StoragePostgreSQL::processNamedCollectionResult(*named_collection, args.context, false);
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
            configuration.database = safeGetLiteralValue<String>(engine_args[1], engine_name);
            configuration.username = safeGetLiteralValue<String>(engine_args[2], engine_name);
            configuration.password = safeGetLiteralValue<String>(engine_args[3], engine_name);
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
    });
}
}

#endif
