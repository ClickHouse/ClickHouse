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
#include <Common/AsyncLoader.h>
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
    , startup_task(getContext()->getSchedulePool().createTask(StorageID::createEmpty(), "MaterializedPostgreSQLDatabaseStartup", [this]{ tryStartSynchronization(); }))
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
    if (CurrentThread::isInitialized() && CurrentThread::get().tryGetQueryContext())
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

SELECT * FROM postgresql_db.postgres_table;
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

    Sets a comma-separated list of PostgreSQL database tables, which will be replicated via [MaterializedPostgreSQL](../../engines/database-engines/materialized-postgresql.md) database engine.

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

    A text string identifying a snapshot, from which [initial dump of PostgreSQL tables](../../engines/database-engines/materialized-postgresql.md) will be performed. Must be used together with `materialized_postgresql_replication_slot`.

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

1. [CREATE PUBLICATION](https://postgrespro.ru/docs/postgresql/14/sql-createpublication) -- create query privilege.

2. [CREATE_REPLICATION_SLOT](https://postgrespro.ru/docs/postgrespro/10/protocol-replication#PROTOCOL-REPLICATION-CREATE-SLOT) -- replication privilege.

3. [pg_drop_replication_slot](https://postgrespro.ru/docs/postgrespro/9.5/functions-admin#functions-replication) -- replication privilege or superuser.

4. [DROP PUBLICATION](https://postgrespro.ru/docs/postgresql/10/sql-droppublication) -- owner of publication (`username` in MaterializedPostgreSQL engine itself).

It is possible to avoid executing `2` and `3` commands and having those permissions. Use settings `materialized_postgresql_replication_slot` and `materialized_postgresql_snapshot`. But with much care.

Access to tables:

1. pg_publication

2. pg_replication_slots

3. pg_publication_tables
)DOCS_MD",
        .syntax = "ENGINE = MaterializedPostgreSQL('host:port', 'database', 'user', 'password')",
        .related = {"PostgreSQL"}});
}
}

#endif
