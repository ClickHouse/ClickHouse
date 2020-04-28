#pragma once

#include <Core/Types.h>
#include <mysqlxx/Connection.h>

namespace DB
{

struct MasterStatusInfo
{
    String binlog_file;
    UInt64 binlog_position;
    String binlog_do_db;
    String binlog_ignore_db;
    String executed_gtid_set;

    MasterStatusInfo(
        String binlog_file_, UInt64 binlog_position_, String binlog_do_db_, String binlog_ignore_db_, String executed_gtid_set_);


};


std::shared_ptr<MasterStatusInfo> fetchMasterStatusInfo(mysqlxx::Connection * connection);

}

