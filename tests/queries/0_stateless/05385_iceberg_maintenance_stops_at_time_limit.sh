#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# `remove_orphan_files` and `expire_snapshots` stopped by `max_execution_time` delete nothing and expire nothing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --send_logs_level=error --allow_insert_into_iceberg=1 --allow_iceberg_remove_orphan_files=1 --allow_experimental_expire_snapshots=1 --allow_experimental_iceberg_compaction=0"
TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}"
ORPHAN="${TABLE_PATH}/data/orphan.parquet"
snapshots() { $CH --query "SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}'"; }

$CH --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0"
for i in 1 2 3; do $CH --query "INSERT INTO ${TABLE} VALUES (${i})"; done
echo "junk" > "${ORPHAN}"
TZ=UTC touch -t 202001010000 "${ORPHAN}"

# The time limit has already passed when the command checks it for the first time.
$CH --max_execution_time=0.00001 --query "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files(older_than = '2023-01-01 00:00:00')" 2>&1 | grep -o -m1 'TIMEOUT_EXCEEDED'
[ -f "${ORPHAN}" ] && echo "orphan kept"
$CH --max_execution_time=0.00001 --query "ALTER TABLE ${TABLE} EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1)" 2>&1 | grep -o -m1 'TIMEOUT_EXCEEDED'
snapshots

# Under `timeout_overflow_mode = 'break'` they stop as well instead of finishing the work.
$CH --max_execution_time=0.00001 --timeout_overflow_mode=break --query "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files(older_than = '2023-01-01 00:00:00')" > /dev/null 2>&1
[ -f "${ORPHAN}" ] && echo "orphan kept"
$CH --max_execution_time=0.00001 --timeout_overflow_mode=break --query "ALTER TABLE ${TABLE} EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1)" > /dev/null 2>&1
snapshots

# Without the limit both commands do their work, so the checks above could have failed.
$CH --query "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files(older_than = '2023-01-01 00:00:00')" > /dev/null
[ -f "${ORPHAN}" ] || echo "orphan deleted"
$CH --query "ALTER TABLE ${TABLE} EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1)" > /dev/null
snapshots

$CH --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH:?}"
