#pragma once

#include "config_core.h"
#if USE_MYSQL

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseOrdinary.h>
#include <Parsers/ASTCreateQuery.h>
#include <mysqlxx/Pool.h>
#include <mutex>

namespace DB
{

class DatabaseMaterializeMySQL : public DatabaseOrdinary
{
public:
    DatabaseMaterializeMySQL(
        const Context & context, const String & database_name_, const String & metadata_path_,
        const ASTStorage * database_engine_define_, const String & mysql_database_name_, mysqlxx::Pool && pool_);

    String getEngineName() const override { return "MySQL"; }

private:
//    const Context & global_context;
//    String metadata_path;
    ASTPtr database_engine_define;
    String mysql_database_name;

    mutable mysqlxx::Pool pool;

    void synchronization();

    void dumpMySQLDatabase();

    void tryToExecuteQuery(const String & query_to_execute);

    String getCreateQuery(const mysqlxx::Pool::Entry & connection, const String & database, const String & table_name);

    mutable std::mutex sync_mutex;
    std::atomic<bool> sync_quit{false};
    std::condition_variable sync_cond;
    ThreadFromGlobalPool thread{&DatabaseMaterializeMySQL::synchronization, this};
};

}

#endif
