#include <Databases/PostgreSQL/DatabaseMaterializedPostgreSQL.h>

#if USE_LIBPQXX

#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseAtomic.h>
#include <Storages/StoragePostgreSQL.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/escapeForFileName.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Common/Macros.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

DatabaseMaterializedPostgreSQL::DatabaseMaterializedPostgreSQL(
        ContextPtr context_,
        const String & metadata_path_,
        UUID uuid_,
        const ASTStorage * database_engine_define_,
        bool is_attach_,
        const String & database_name_,
        const String & postgres_database_name,
        const postgres::ConnectionInfo & connection_info_,
        std::unique_ptr<MaterializedPostgreSQLSettings> settings_)
    : DatabaseAtomic(database_name_, metadata_path_, uuid_, "DatabaseMaterializedPostgreSQL (" + database_name_ + ")", context_)
    , database_engine_define(database_engine_define_->clone())
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
            *settings,
            /* is_materialized_postgresql_database = */ true);

    postgres::Connection connection(connection_info);
    std::set<String> tables_to_replicate;
    try
    {
        tables_to_replicate = replication_handler->fetchRequiredTables(connection);
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
            storage = StorageMaterializedPostgreSQL::create(storage, getContext());
        }
        else
        {
            /// Nested table does not exist and will be created by replication thread.
            storage = StorageMaterializedPostgreSQL::create(StorageID(database_name, table_name), getContext());
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

    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "Create table query allowed only for ReplacingMergeTree engine and from synchronization thread");
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
        ContextPtr local_context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name)
{
    /// Modify context into nested_context and pass query to Atomic database.
    return DatabaseAtomic::getTablesIterator(StorageMaterializedPostgreSQL::makeNestedTableContext(local_context), filter_by_table_name);
}


}

#endif
