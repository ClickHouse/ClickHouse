#pragma once


#if !defined(ARCADIA_BUILD)
    #include "config_core.h"
# endif

#if USE_MYSQL

#include <memory>
#include <unordered_map>

#include <DataTypes/DataTypeString.h>
#include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#include <Interpreters/Context.h>
#include <mysqlxx/Connection.h>
#include <mysqlxx/PoolWithFailover.h>

namespace DB
{

using namespace MySQLReplicaConsumer;

void dumpTables(
    const Context & global_context,
    mysqlxx::Pool::Entry & connection,
    Poco::Logger * log,
    std::shared_ptr<const ConsumerDatabase> consumer,
    std::unordered_map<String, String> & need_dumping_tables,
    const std::function<bool()> & is_cancelled);

std::unordered_map<String, String> fetchTablesCreateQuery(
    const mysqlxx::PoolWithFailover::Entry & connection,
    const String & mysql_database_name);

}

#endif
