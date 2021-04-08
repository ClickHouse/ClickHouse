#include <Databases/PostgreSQL/DatabaseMaterializePostgreSQL.h>

#if USE_LIBPQXX

#include <Storages/PostgreSQL/PostgreSQLConnection.h>
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

template<>
DatabaseMaterializePostgreSQL<DatabaseOrdinary>::DatabaseMaterializePostgreSQL(
        const Context & context,
        const String & metadata_path_,
        UUID /* uuid */,
        const ASTStorage * database_engine_define_,
        const String & database_name_,
        const String & postgres_database_name,
        const String & connection_string,
        std::unique_ptr<MaterializePostgreSQLSettings> settings_)
    : DatabaseOrdinary(
            database_name_, metadata_path_, "data/" + escapeForFileName(database_name_) + "/",
            "DatabaseMaterializePostgreSQL<Ordinary> (" + database_name_ + ")", context)
    , log(&Poco::Logger::get("MaterializePostgreSQLDatabaseEngine"))
    , global_context(context.getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , database_name(database_name_)
    , remote_database_name(postgres_database_name)
    , connection(std::make_shared<postgres::Connection>(connection_string, ""))
    , settings(std::move(settings_))
{
}


template<>
DatabaseMaterializePostgreSQL<DatabaseAtomic>::DatabaseMaterializePostgreSQL(
        const Context & context,
        const String & metadata_path_,
        UUID uuid,
        const ASTStorage * database_engine_define_,
        const String & database_name_,
        const String & postgres_database_name,
        const String & connection_string,
        std::unique_ptr<MaterializePostgreSQLSettings> settings_)
    : DatabaseAtomic(database_name_, metadata_path_, uuid, "DatabaseMaterializePostgreSQL<Atomic> (" + database_name_ + ")", context)
    , global_context(context.getGlobalContext())
    , metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone())
    , remote_database_name(postgres_database_name)
    , connection(std::make_shared<postgres::Connection>(connection_string, ""))
    , settings(std::move(settings_))
{
}


template<typename Base>
void DatabaseMaterializePostgreSQL<Base>::startSynchronization()
{
    replication_handler = std::make_unique<PostgreSQLReplicationHandler>(
            remote_database_name,
            connection->getConnectionString(),
            metadata_path + METADATA_SUFFIX,
            global_context,
            settings->postgresql_replica_max_block_size.value,
            settings->postgresql_replica_allow_minimal_ddl, true,
            settings->postgresql_replica_tables_list.value);

    std::unordered_set<std::string> tables_to_replicate = replication_handler->fetchRequiredTables(connection->getRef());

    for (const auto & table_name : tables_to_replicate)
    {
        auto storage = tryGetTable(table_name, global_context);

        if (!storage)
        {
            storage = StorageMaterializePostgreSQL::create(StorageID(database_name, table_name), StoragePtr{}, global_context);
        }

        replication_handler->addStorage(table_name, storage->template as<StorageMaterializePostgreSQL>());
        tables[table_name] = storage;
    }

    LOG_TRACE(log, "Loaded {} tables. Starting synchronization", tables.size());
    replication_handler->startup();
}


template<typename Base>
void DatabaseMaterializePostgreSQL<Base>::shutdown()
{
    if (replication_handler)
        replication_handler->shutdown();
}


template<typename Base>
void DatabaseMaterializePostgreSQL<Base>::loadStoredObjects(
        Context & context, bool has_force_restore_data_flag, bool force_attach)
{
    Base::loadStoredObjects(context, has_force_restore_data_flag, force_attach);

    try
    {
        startSynchronization();
    }
    catch (...)
    {
        tryLogCurrentException(Base::log, "Cannot load nested database objects for PostgreSQL database engine.");

        if (!force_attach)
            throw;
    }

}


template<typename Base>
StoragePtr DatabaseMaterializePostgreSQL<Base>::tryGetTable(const String & name, const Context & context) const
{
    /// When a nested ReplacingMergeTree table is managed from PostgreSQLReplicationHandler, its context is modified
    /// to show the type of managed table.
    if (context.hasQueryContext())
    {
        auto storage_set = context.getQueryContext().getQueryFactoriesInfo().storages;
        if (storage_set.find("ReplacingMergeTree") != storage_set.end())
        {
            return Base::tryGetTable(name, context);
        }
    }

    auto table = tables.find(name);
    /// Here it is possible that nested table is temporarily out of reach, but return storage anyway,
    /// it will not allow to read if nested is unavailable at the moment
    if (table != tables.end())
        return table->second;

    return StoragePtr{};
}


template<typename Base>
void DatabaseMaterializePostgreSQL<Base>::createTable(const Context & context, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    if (context.hasQueryContext())
    {
        auto storage_set = context.getQueryContext().getQueryFactoriesInfo().storages;
        if (storage_set.find("ReplacingMergeTree") != storage_set.end())
        {
            Base::createTable(context, name, table, query);
            return;
        }
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "Create table query allowed only for ReplacingMergeTree engine and from synchronization thread");
}


template<typename Base>
void DatabaseMaterializePostgreSQL<Base>::drop(const Context & context)
{
    if (replication_handler)
    {
        replication_handler->shutdown();
        replication_handler->shutdownFinal();
    }

    /// Remove metadata
    Poco::File metadata(Base::getMetadataPath() + METADATA_SUFFIX);

    if (metadata.exists())
        metadata.remove(false);

    Base::drop(context);
}


template<typename Base>
DatabaseTablesIteratorPtr DatabaseMaterializePostgreSQL<Base>::getTablesIterator(
        const Context & /* context */, const DatabaseOnDisk::FilterByNameFunction & /* filter_by_table_name */)
{
    Tables nested_tables;
    for (const auto & [table_name, storage] : tables)
    {
        auto nested_storage = storage->template as<StorageMaterializePostgreSQL>()->tryGetNested();

        if (nested_storage)
            nested_tables[table_name] = nested_storage;
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(nested_tables, database_name);
}

}

#endif
