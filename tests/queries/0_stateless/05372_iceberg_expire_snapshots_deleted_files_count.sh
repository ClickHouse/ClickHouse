#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# `expire_snapshots` must count a file as deleted only if its removal succeeded, count each removal error in `failed_deletions_count`, and count each expired file once.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

local_query() { ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --allow_experimental_expire_snapshots=1 -q "$1" -- --user_files_path="${WORK_DIR}"; }

# $1: table name, $2: statements run after CREATE in the first process, $3: statement run right before expire_snapshots.
expire()
{
    local path="${WORK_DIR}/$1/"
    local_query "
        CREATE TABLE t (id Int64, p Int64) ENGINE = IcebergLocal('${path}') PARTITION BY p
            SETTINGS allow_experimental_iceberg_compaction = 0;
        $2"
    local_query "
        CREATE TABLE t ENGINE = IcebergLocal('${path}');
        $3
        ALTER TABLE t EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1);"
}

# The second snapshot drops partition 2, so expiring the first one deletes one data file, one manifest and one manifest list.
DROP_ONE="
        INSERT INTO t VALUES (1, 1), (2, 2);
        ALTER TABLE t DROP PARTITION 2;"

echo "all deletes succeed"
expire t1 "${DROP_ONE}" ""
echo "the first delete call throws"
expire t2 "${DROP_ONE}" "SYSTEM ENABLE FAILPOINT local_object_storage_network_error_during_remove;"
echo "a data file listed in two expired manifests"
# OPTIMIZE ... MANIFEST lists both data files in one more manifest, and DROP PARTITION then removes all of them.
expire t3 "
        INSERT INTO t VALUES (1, 1);
        INSERT INTO t VALUES (2, 1);
        OPTIMIZE TABLE t MANIFEST SETTINGS allow_experimental_iceberg_compaction = 1, iceberg_manifest_min_count_to_compact = 1;
        ALTER TABLE t DROP PARTITION 1;" ""
