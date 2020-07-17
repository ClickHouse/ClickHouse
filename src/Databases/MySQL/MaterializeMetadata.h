#pragma once

#include <Core/Types.h>
#include <Core/MySQLReplication.h>
#include <mysqlxx/Connection.h>
#include <mysqlxx/PoolWithFailover.h>

namespace DB
{

struct MaterializeMetadata
{
    const String persistent_path;

    String binlog_file;
    UInt64 binlog_position;
    String binlog_do_db;
    String binlog_ignore_db;
    String executed_gtid_set;

    size_t version = 0;
    std::unordered_map<String, String> need_dumping_tables;

    void fetchMasterStatus(mysqlxx::PoolWithFailover::Entry & connection);

    bool checkBinlogFileExists(mysqlxx::PoolWithFailover::Entry & connection, const String & mysql_version) const;

    void transaction(const MySQLReplication::Position & position, const std::function<void()> & fun);

    MaterializeMetadata(
        mysqlxx::PoolWithFailover::Entry & connection, const String & path
        , const String & database, bool & opened_transaction, const String & mysql_version);
};

}

