#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <common/types.h>
#include <Core/MySQL/MySQLReplication.h>
#include <mysqlxx/Connection.h>
#include <mysqlxx/PoolWithFailover.h>
#include <Interpreters/Context.h>

namespace DB
{

/** Materialize database engine metadata
 *
 * Record data version and current snapshot of MySQL, including:
 * binlog_file  - currently executing binlog_file
 * binlog_position  - position of the currently executing binlog file
 * executed_gtid_set - currently executing gtid
 * need_dumping_tables - Table structure snapshot at the current moment(Only when database first created or executed binlog file is deleted)
 */
struct MaterializeMetadata
{
    const String persistent_path;
    const Settings settings;

    String binlog_file;
    UInt64 binlog_position;
    String binlog_do_db;
    String binlog_ignore_db;
    String executed_gtid_set;

    size_t data_version = 1;
    size_t meta_version = 2;
    String binlog_checksum = "CRC32";

    void fetchMasterStatus(mysqlxx::PoolWithFailover::Entry & connection);

    void fetchMasterVariablesValue(const mysqlxx::PoolWithFailover::Entry & connection);

    bool checkBinlogFileExists(const mysqlxx::PoolWithFailover::Entry & connection) const;

    void transaction(const MySQLReplication::Position & position, const std::function<void()> & fun);

    void startReplication(
        mysqlxx::PoolWithFailover::Entry & connection,
        const String & database,
        bool & opened_transaction,
        std::unordered_map<String, String> & need_dumping_tables);

    MaterializeMetadata(const String & path_, const Settings & settings_);
};

}

#endif
