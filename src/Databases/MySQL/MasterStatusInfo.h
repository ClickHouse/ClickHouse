#pragma once

#include <Core/Types.h>
#include <mysqlxx/Connection.h>
#include <mysqlxx/PoolWithFailover.h>

namespace DB
{

struct MasterStatusInfo
{
    String binlog_file;
    UInt64 binlog_position;
    String binlog_do_db;
    String binlog_ignore_db;
    String executed_gtid_set;

    std::vector<String> need_dumping_tables;

    MasterStatusInfo(mysqlxx::PoolWithFailover::Entry & connection, const String & database);

    void fetchMasterStatus(mysqlxx::PoolWithFailover::Entry & connection);


};

}

