#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Core/Block.h>
#include <common/types.h>
#include <Core/MySQL/MySQLReplication.h>
#include <mysqlxx/Connection.h>
#include <mysqlxx/PoolWithFailover.h>

namespace DB
{

/** Materialize database engine metadata
 *
 * Record data version and current snapshot of MySQL, including:
 * binlog_file  - currently executing binlog_file
 * binlog_position  - position of the currently executing binlog file
 * executed_gtid_set - currently executing gtid
 */
struct MaterializeMetadata
{
    const String persistent_path;

    String binlog_file;
    UInt64 binlog_position;
    String binlog_do_db;
    String binlog_ignore_db;
    String executed_gtid_set;

    size_t data_version = 1;
    size_t meta_version = 2;
    String mysql_version;

    bool is_initialized;

    void fetchMasterStatus(mysqlxx::PoolWithFailover::Entry & connection);

    bool checkBinlogFileExists(mysqlxx::PoolWithFailover::Entry & connection) const;

    Block getShowMasterLogHeader() const;

    void commitMetadata(const std::function<void()> & function, const String & persistent_tmp_path);

    void transaction(const MySQLReplication::Position & position, const std::function<void()> & fun);

    bool tryInitFromFile(mysqlxx::PoolWithFailover::Entry & connection);

    void fetchMetadata(mysqlxx::PoolWithFailover::Entry & connection);

    MaterializeMetadata(
        mysqlxx::PoolWithFailover::Entry & connection,
        const String & path,
        const String & mysql_version_);

    MaterializeMetadata();
};

using MaterializeMetadataPtr = std::shared_ptr<MaterializeMetadata>;

}

#endif
