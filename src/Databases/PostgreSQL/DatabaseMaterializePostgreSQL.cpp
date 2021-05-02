#include <Databases/PostgreSQL/DatabaseMaterializePostgreSQL.h>

#if USE_LIBPQXX

#include <Core/PostgreSQL/PostgreSQLConnection.h>
#include <Storages/PostgreSQL/StorageMaterializePostgreSQL.h>

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
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Common/Macros.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

static const auto METADATA_SUFFIX = ".postgresql_replica_metadata";

DatabaseMaterializePostgreSQL::DatabaseMaterializePostgreSQL(
        ContextPtr context_,
        const String & metadata_path_,
        UUID uuid_,
        const ASTStorage * database_engine_define_,
        const String & database_name_,
        const String & postgres_database_name,
        const postgres::ConnectionInfo & connection_info,
        std::unique_ptr<MaterializePostgreSQLSettings> settings_)
    : DatabaseAtomic(database_name_, metadata_path_, uuid_, "DatabaseMaterializePostgreSQL<Atomic> (" + database_name_ + ")", context_)
    , database_engine_define(database_engine_define_->clone())
    , remote_database_name(postgres_database_name)
    , connection(std::make_shared<postgres::Connection>(connection_info))
    , settings(std::move(settings_))
{
}


void DatabaseMaterializePostgreSQL::startSynchronization()
{
    replication_handler = std::make_unique<PostgreSQLReplicationHandler>(
            remote_database_name,
            database_name,
            connection->getConnectionInfo(),
            metadata_path + METADATA_SUFFIX,
            getContext(),
            settings->postgresql_replica_max_block_size.value,
            settings->postgresql_replica_allow_minimal_ddl,
            /* is_materialize_postgresql_database = */ true,
            settings->postgresql_replica_tables_list.value);

    std::unordered_set<std::string> tables_to_replicate = replication_handler->fetchRequiredTables(connection->getRef());

    for (const auto & table_name : tables_to_replicate)
    {
        auto storage = tryGetTable(table_name, getContext());

        if (!storage)
            storage = StorageMaterializePostgreSQL::create(StorageID(database_name, table_name), getContext());

        replication_handler->addStorage(table_name, storage->as<StorageMaterializePostgreSQL>());
        materialized_tables[table_name] = storage;
    }

    LOG_TRACE(log, "Loaded {} tables. Starting synchronization", materialized_tables.size());
    replication_handler->startup();
}


void DatabaseMaterializePostgreSQL::shutdown()
{
    if (replication_handler)
        replication_handler->shutdown();
}


void DatabaseMaterializePostgreSQL::loadStoredObjects(ContextPtr local_context, bool has_force_restore_data_flag, bool force_attach)
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


StoragePtr DatabaseMaterializePostgreSQL::tryGetTable(const String & name, ContextPtr local_context) const
{
    /// When a nested ReplacingMergeTree table is managed from PostgreSQLReplicationHandler, its context is modified
    /// to show the type of managed table.
    if (local_context->hasQueryContext())
    {
        auto storage_set = local_context->getQueryContext()->getQueryFactoriesInfo().storages;
        if (storage_set.find("ReplacingMergeTree") != storage_set.end())
        {
            return DatabaseAtomic::tryGetTable(name, local_context);
        }
    }

    /// Note: In select query we call MaterializePostgreSQL table and it calls tryGetTable from its nested.
    std::lock_guard lock(tables_mutex);
    auto table = materialized_tables.find(name);

    /// Nested table is not created immediately. Consider that table exists only if nested table exists.
    if (table != materialized_tables.end() && table->second->as<StorageMaterializePostgreSQL>()->isNestedLoaded())
        return table->second;

    return StoragePtr{};
}


void DatabaseMaterializePostgreSQL::createTable(ContextPtr local_context, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    if (local_context->hasQueryContext())
    {
        auto storage_set = local_context->getQueryContext()->getQueryFactoriesInfo().storages;
        if (storage_set.find("ReplacingMergeTree") != storage_set.end())
        {
            DatabaseAtomic::createTable(local_context, name, table, query);
            return;
        }
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "Create table query allowed only for ReplacingMergeTree engine and from synchronization thread");
}


void DatabaseMaterializePostgreSQL::stopReplication()
{
    if (replication_handler)
        replication_handler->shutdown();
}


void DatabaseMaterializePostgreSQL::drop(ContextPtr local_context)
{
    if (replication_handler)
        replication_handler->shutdownFinal();

    /// Remove metadata
    Poco::File metadata(getMetadataPath() + METADATA_SUFFIX);

    if (metadata.exists())
        metadata.remove(false);

    DatabaseAtomic::drop(local_context);
}


DatabaseTablesIteratorPtr DatabaseMaterializePostgreSQL::getTablesIterator(
        ContextPtr /* context */, const DatabaseOnDisk::FilterByNameFunction & /* filter_by_table_name */)
{
    Tables nested_tables;
    for (const auto & [table_name, storage] : materialized_tables)
    {
        auto nested_storage = storage->as<StorageMaterializePostgreSQL>()->tryGetNested();

        if (nested_storage)
            nested_tables[table_name] = nested_storage;
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(nested_tables, database_name);
}


}

#endif
