#!/usr/bin/env bash
# An INSERT that has written its files but not yet committed must survive maintenance:
# `remove_orphan_files` with the default window keeps its files, and an `expire_snapshots`
# commit in between makes the INSERT retry its commit instead of losing rows.
#
# Tags: no-fasttest, no-parallel, no-replicated-database
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: failpoints are server-wide and would pause INSERTs of other tests

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading format version 3 metadata logs a spurious "violates Iceberg specification" warning,
# https://github.com/ClickHouse/ClickHouse/issues/125333
CH="${CLICKHOUSE_CLIENT} --send_logs_level=error --allow_insert_into_iceberg=1 --allow_iceberg_remove_orphan_files=1 --allow_experimental_expire_snapshots=1"

for format_version in 2 3; do
    for command in "remove_orphan_files()" "expire_snapshots(retention_period = '1ms', retain_last = 1)"; do
        TABLE="t_${CLICKHOUSE_DATABASE}_${command%%(*}_v${format_version}"
        TABLE_PATH="${USER_FILES_PATH}/${TABLE}"
        $CH --query "
            CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet')
            SETTINGS iceberg_format_version = ${format_version}, allow_experimental_iceberg_compaction = 0;
            INSERT INTO ${TABLE} VALUES (1);
            INSERT INTO ${TABLE} VALUES (2);"

        $CH --query "SYSTEM ENABLE FAILPOINT iceberg_writes_pause_before_commit"
        $CH --query "INSERT INTO ${TABLE} VALUES (3)" &
        $CH --query "SYSTEM WAIT FAILPOINT iceberg_writes_pause_before_commit PAUSE"

        echo "${command%%(*}, format version ${format_version}"
        $CH --query "ALTER TABLE ${TABLE} EXECUTE ${command}" | grep -v $'\t0$'

        $CH --query "SYSTEM DISABLE FAILPOINT iceberg_writes_pause_before_commit"
        wait
        $CH --query "SELECT arraySort(groupArray(id)) FROM ${TABLE}"

        $CH --query "DROP TABLE ${TABLE}"
        rm -rf "${TABLE_PATH}"
    done
done
