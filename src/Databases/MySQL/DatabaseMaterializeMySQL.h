#pragma once

#include "config_core.h"

#if USE_MYSQL

#    include <mutex>
#    include <Core/MySQLClient.h>
#    include <DataStreams/BlockIO.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <Databases/DatabaseOrdinary.h>
#    include <Databases/IDatabase.h>
#    include <Databases/MySQL/DatabaseMaterializeMySQLWrap.h>
#    include <Databases/MySQL/MaterializeMetadata.h>
#    include <Databases/MySQL/MaterializeModeSettings.h>
#    include <Interpreters/MySQL/CreateQueryVisitor.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <mysqlxx/Pool.h>
#    include <mysqlxx/PoolWithFailover.h>

namespace DB
{

class DatabaseMaterializeMySQL : public DatabaseMaterializeMySQLWrap
{
public:
    ~DatabaseMaterializeMySQL() override;

    DatabaseMaterializeMySQL(
        const Context & context, const String & database_name_, const String & metadata_path_,
        const ASTStorage * database_engine_define_, const String & mysql_database_name_, mysqlxx::Pool && pool_,
        MySQLClient && client_, std::unique_ptr<MaterializeModeSettings> settings_);

    String getEngineName() const override { return "MySQL"; }

private:
    const Context & global_context;
    String metadata_path;
    String mysql_database_name;

    mutable mysqlxx::Pool pool;
    mutable MySQLClient client;
    std::unique_ptr<MaterializeModeSettings> settings;

    void cleanOutdatedTables();

    void scheduleSynchronized();

    BlockOutputStreamPtr getTableOutput(const String & table_name);

    std::optional<MaterializeMetadata> prepareSynchronized(std::unique_lock<std::mutex> & lock, const std::function<bool()> & is_cancelled);

    void dumpDataForTables(mysqlxx::Pool::Entry & connection, MaterializeMetadata & master_info, const std::function<bool()> & is_cancelled);

    std::mutex sync_mutex;
    std::atomic<bool> sync_quit{false};
    std::condition_variable sync_cond;
    ThreadPool background_thread_pool{1};
};

}

#endif
