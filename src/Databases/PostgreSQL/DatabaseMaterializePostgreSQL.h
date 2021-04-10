#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX

#include <Storages/PostgreSQL/PostgreSQLReplicationHandler.h>
#include <Storages/PostgreSQL/MaterializePostgreSQLSettings.h>

#include <Databases/DatabasesCommon.h>
#include <Core/BackgroundSchedulePool.h>
#include <Parsers/ASTCreateQuery.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseAtomic.h>


namespace DB
{

class Context;
class PostgreSQLConnection;
using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;


class DatabaseMaterializePostgreSQL : public DatabaseAtomic
{

public:
    DatabaseMaterializePostgreSQL(
        const Context & context_,
        const String & metadata_path_,
        UUID uuid_,
        const ASTStorage * database_engine_define_,
        const String & database_name_,
        const String & postgres_database_name,
        const postgres::ConnectionInfo & connection_info,
        std::unique_ptr<MaterializePostgreSQLSettings> settings_);

    String getEngineName() const override { return "MaterializePostgreSQL"; }

    String getMetadataPath() const override { return metadata_path; }

    void loadStoredObjects(Context &, bool, bool force_attach) override;

    DatabaseTablesIteratorPtr getTablesIterator(
            const Context & context, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name) override;

    StoragePtr tryGetTable(const String & name, const Context & context) const override;

    void createTable(const Context & context, const String & name, const StoragePtr & table, const ASTPtr & query) override;

    void drop(const Context & context) override;

    void shutdown() override;

    void stopReplication();

private:
    void startSynchronization();

    ASTPtr database_engine_define;
    String remote_database_name;
    postgres::ConnectionPtr connection;
    std::unique_ptr<MaterializePostgreSQLSettings> settings;

    std::shared_ptr<PostgreSQLReplicationHandler> replication_handler;
    std::map<std::string, StoragePtr> materialized_tables;
    mutable std::mutex tables_mutex;
};

}

#endif
