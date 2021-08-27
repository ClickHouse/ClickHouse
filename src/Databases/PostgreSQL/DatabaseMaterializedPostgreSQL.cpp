#include <Databases/PostgreSQL/DatabaseMaterializedPostgreSQL.h>

#if USE_LIBPQXX

#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>

#include <common/logger_useful.h>
#include <Common/Macros.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseAtomic.h>
#include <Storages/StoragePostgreSQL.h>
#include <Storages/AlterCommands.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Common/escapeForFileName.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_NOT_ALLOWED;
}

DatabaseMaterializedPostgreSQL::DatabaseMaterializedPostgreSQL(
        ContextPtr context_,
        const String & metadata_path_,
        UUID uuid_,
        ASTPtr storage_def_,
        bool is_attach_,
        const String & database_name_,
        const String & postgres_database_name,
        const postgres::ConnectionInfo & connection_info_,
        std::unique_ptr<MaterializedPostgreSQLSettings> settings_)
    : DatabaseAtomic(database_name_, metadata_path_, uuid_, "DatabaseMaterializedPostgreSQL (" + database_name_ + ")", context_, storage_def_)
    , is_attach(is_attach_)
    , remote_database_name(postgres_database_name)
    , connection_info(connection_info_)
    , settings(std::move(settings_))
{
}


void DatabaseMaterializedPostgreSQL::startSynchronization()
{
    replication_handler = std::make_unique<PostgreSQLReplicationHandler>(
            /* replication_identifier */database_name,
            remote_database_name,
            database_name,
            connection_info,
            getContext(),
            is_attach,
            settings->materialized_postgresql_max_block_size.value,
            settings->materialized_postgresql_allow_automatic_update,
            /* is_materialized_postgresql_database = */ true,
            settings->materialized_postgresql_tables_list.value);

    NameSet tables_to_replicate;
    try
    {
        tables_to_replicate = replication_handler->fetchRequiredTables();
    }
    catch (...)
    {
        LOG_ERROR(log, "Unable to load replicated tables list");
        throw;
    }

    if (tables_to_replicate.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty list of tables to replicate");

    for (const auto & table_name : tables_to_replicate)
    {
        /// Check nested ReplacingMergeTree table.
        auto storage = DatabaseAtomic::tryGetTable(table_name, getContext());

        if (storage)
        {
            /// Nested table was already created and synchronized.
            storage = StorageMaterializedPostgreSQL::create(storage, getContext(), remote_database_name, table_name);
        }
        else
        {
            /// Nested table does not exist and will be created by replication thread.
            storage = StorageMaterializedPostgreSQL::create(StorageID(database_name, table_name), getContext(), remote_database_name, table_name);
        }

        /// Cache MaterializedPostgreSQL wrapper over nested table.
        materialized_tables[table_name] = storage;

        /// Let replication thread know, which tables it needs to keep in sync.
        replication_handler->addStorage(table_name, storage->as<StorageMaterializedPostgreSQL>());
    }

    LOG_TRACE(log, "Loaded {} tables. Starting synchronization", materialized_tables.size());
    replication_handler->startup();
}


void DatabaseMaterializedPostgreSQL::loadStoredObjects(ContextMutablePtr local_context, bool has_force_restore_data_flag, bool force_attach)
{
    DatabaseAtomic::loadStoredObjects(local_context, has_force_restore_data_flag, force_attach);

    try
    {
        startSynchronization();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot load nested database objects for PostgreSQL database engine.");

        if (!force_attach)
            throw;
    }
}


void DatabaseMaterializedPostgreSQL::checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const
{
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::MODIFY_DATABASE_SETTING)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by database engine {}", alterTypeToString(command.type), getEngineName());
    }
}


void DatabaseMaterializedPostgreSQL::applySettings(const SettingsChanges & settings_changes, ContextPtr local_context)
{
    for (const auto & change : settings_changes)
    {
        if (!settings->has(change.name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine {} does not support setting `{}`", getEngineName(), change.name);

        if ((change.name == "materialized_postgresql_tables_list"))
        {
            if (!local_context->isInternalQuery())
                throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Changing setting `{}` is not allowed", change.name);
        }
        else if (change.name == "materialized_postgresql_allow_automatic_update")
        {
        }
        else if (change.name == "materialized_postgresql_max_block_size")
        {
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting");
        }

        settings->applyChange(change);
    }
}


StoragePtr DatabaseMaterializedPostgreSQL::tryGetTable(const String & name, ContextPtr local_context) const
{
    /// In otder to define which table access is needed - to MaterializedPostgreSQL table (only in case of SELECT queries) or
    /// to its nested ReplacingMergeTree table (in all other cases), the context of a query os modified.
    /// Also if materialzied_tables set is empty - it means all access is done to ReplacingMergeTree tables - it is a case after
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
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "CREATE TABLE is not allowed for database engine {}. Use ATTACH TABLE instead", getEngineName());

    /// Create ReplacingMergeTree table.
    auto query_copy = query->clone();
    auto create_query = assert_cast<ASTCreateQuery *>(query_copy.get());
    create_query->attach = false;
    create_query->attach_short_syntax = false;
    DatabaseAtomic::createTable(StorageMaterializedPostgreSQL::makeNestedTableContext(local_context), table_name, table, query_copy);

    /// Attach MaterializedPostgreSQL table.
    attachTable(table_name, table, {});
}


String DatabaseMaterializedPostgreSQL::getTablesList() const
{
    String tables_list;
    for (const auto & table : materialized_tables)
    {
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

    auto storage = StorageMaterializedPostgreSQL::create(StorageID(database_name, table_name), getContext(), remote_database_name, table_name);
    auto ast_storage = replication_handler->getCreateNestedTableQuery(storage.get(), table_name);
    assert_cast<ASTCreateQuery *>(ast_storage.get())->uuid = UUIDHelpers::generateV4();
    return ast_storage;
}


void DatabaseMaterializedPostgreSQL::attachTable(const String & table_name, const StoragePtr & table, const String & relative_table_path)
{
    if (CurrentThread::isInitialized() && CurrentThread::get().getQueryContext())
    {
        auto set = std::make_shared<ASTSetQuery>();
        set->is_standalone = false;

        auto tables_to_replicate = settings->materialized_postgresql_tables_list.value;
        if (tables_to_replicate.empty())
            tables_to_replicate = getTablesList();

        /// tables_to_replicate can be empty if postgres database had no tables when this database was created.
        set->changes = {SettingChange("materialized_postgresql_tables_list",
                                      tables_to_replicate.empty() ? table_name : (tables_to_replicate + "," + table_name))};

        auto command = std::make_shared<ASTAlterCommand>();
        command->type = ASTAlterCommand::Type::MODIFY_DATABASE_SETTING;
        command->settings_changes = std::move(set);

        auto command_list = std::make_shared<ASTExpressionList>();
        command_list->children.push_back(command);

        auto query = std::make_shared<ASTAlterQuery>();
        auto * alter = query->as<ASTAlterQuery>();

        alter->alter_object = ASTAlterQuery::AlterObjectType::DATABASE;
        alter->database = database_name;
        alter->set(alter->command_list, command_list);

        auto current_context = Context::createCopy(getContext()->getGlobalContext());
        current_context->setInternalQuery(true);
        InterpreterAlterQuery(query, current_context).execute();

        auto storage = StorageMaterializedPostgreSQL::create(table, getContext(), remote_database_name, table_name);
        materialized_tables[table_name] = storage;
        replication_handler->addTableToReplication(dynamic_cast<StorageMaterializedPostgreSQL *>(storage.get()), table_name);
    }
    else
    {
        DatabaseAtomic::attachTable(table_name, table, relative_table_path);
    }
}


void DatabaseMaterializedPostgreSQL::shutdown()
{
    stopReplication();
    DatabaseAtomic::shutdown();
}


void DatabaseMaterializedPostgreSQL::stopReplication()
{
    if (replication_handler)
        replication_handler->shutdown();

    /// Clear wrappers over nested, all access is not done to nested tables directly.
    materialized_tables.clear();
}


void DatabaseMaterializedPostgreSQL::dropTable(ContextPtr local_context, const String & table_name, bool no_delay)
{
    /// Modify context into nested_context and pass query to Atomic database.
    DatabaseAtomic::dropTable(StorageMaterializedPostgreSQL::makeNestedTableContext(local_context), table_name, no_delay);
}


void DatabaseMaterializedPostgreSQL::drop(ContextPtr local_context)
{
    if (replication_handler)
        replication_handler->shutdownFinal();

    DatabaseAtomic::drop(StorageMaterializedPostgreSQL::makeNestedTableContext(local_context));
}


DatabaseTablesIteratorPtr DatabaseMaterializedPostgreSQL::getTablesIterator(
    ContextPtr local_context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name) const
{
    /// Modify context into nested_context and pass query to Atomic database.
    return DatabaseAtomic::getTablesIterator(StorageMaterializedPostgreSQL::makeNestedTableContext(local_context), filter_by_table_name);
}

}

#endif
