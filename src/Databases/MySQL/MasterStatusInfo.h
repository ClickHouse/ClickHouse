#pragma once

#include <Core/Types.h>
#include <Core/MySQLReplication.h>
#include <mysqlxx/Connection.h>
#include <mysqlxx/PoolWithFailover.h>

namespace DB
{

struct MasterStatusInfo
{
    const String persistent_path;

    String binlog_file;
    UInt64 binlog_position;
    String binlog_do_db;
    String binlog_ignore_db;
    String executed_gtid_set;

    std::vector<String> need_dumping_tables;

    void finishDump();

    void fetchMasterStatus(mysqlxx::PoolWithFailover::Entry & connection);

    bool checkBinlogFileExists(mysqlxx::PoolWithFailover::Entry & connection);

    void transaction(const MySQLReplication::Position & position, const std::function<void()> & fun);

    MasterStatusInfo(mysqlxx::PoolWithFailover::Entry & connection, const String & path, const String & database);


};

}

