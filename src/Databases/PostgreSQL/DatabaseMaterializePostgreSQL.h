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


namespace DB
{

class Context;
class PostgreSQLConnection;
using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;


template<typename Base>
class DatabaseMaterializePostgreSQL : public Base
{

public:
    DatabaseMaterializePostgreSQL(
        const Context & context,
        const String & metadata_path_,
        UUID uuid,
        const ASTStorage * database_engine_define,
        const String & dbname_,
        const String & postgres_dbname,
        const String & connection_string,
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

private:
    void startSynchronization();

    Poco::Logger * log;
    const Context & global_context;
    String metadata_path;
    ASTPtr database_engine_define;

    String database_name, remote_database_name;
    postgres::ConnectionPtr connection;
    std::unique_ptr<MaterializePostgreSQLSettings> settings;

    std::shared_ptr<PostgreSQLReplicationHandler> replication_handler;
    std::map<std::string, StoragePtr> tables;
};

}

#endif
