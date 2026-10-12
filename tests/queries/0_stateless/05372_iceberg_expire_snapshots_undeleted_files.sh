#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# `expire_snapshots` must count only the files it deleted and report the ones it failed to delete.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

local_query() { ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --allow_experimental_expire_snapshots=1 -q "$1" -- --user_files_path="${WORK_DIR}"; }

# $1: table name, $2: statement run right before expire_snapshots.
# The second snapshot drops partition 2, so expiring the first one deletes one data file, one manifest and one manifest list.
expire()
{
    local path="${WORK_DIR}/$1/"
    local_query "
        CREATE TABLE t (id Int64, p Int64) ENGINE = IcebergLocal('${path}') PARTITION BY p
            SETTINGS allow_experimental_iceberg_compaction = 0;
        INSERT INTO t VALUES (1, 1), (2, 2);
        ALTER TABLE t DROP PARTITION 2;"
    local_query "
        CREATE TABLE t ENGINE = IcebergLocal('${path}');
        $2
        ALTER TABLE t EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1);"
}

echo "all deletes succeed"
expire t1 ""
echo "the first delete fails"
expire t2 "SYSTEM ENABLE FAILPOINT local_object_storage_network_error_during_remove;"
