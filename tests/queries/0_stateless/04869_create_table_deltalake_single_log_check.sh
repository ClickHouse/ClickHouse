#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# A fresh CREATE TABLE ... ENGINE = DeltaLake must probe the `_delta_log` exactly once. The check in
# `createTable` reuses the result computed in `createInitial` instead of listing again (the second listing
# only happens on a lost-create race). The ProfileEvent `DeltaLakeDeltaLogExistenceChecks` counts the probes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_single_log_check"
QUERY_ID="${CLICKHOUSE_DATABASE}_single_log_check"

rm -rf "$TABLE_PATH"

$CLICKHOUSE_CLIENT \
    --allow_experimental_delta_lake_writes=1 \
    --allow_delta_lake_create_table=1 \
    --query_id "$QUERY_ID" \
    --query "CREATE TABLE t_dl_single (id Int32, name String) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet)"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# Exactly one `_delta_log` existence check for the fresh create.
$CLICKHOUSE_CLIENT --query "
SELECT ProfileEvents['DeltaLakeDeltaLogExistenceChecks']
FROM system.query_log
WHERE query_id = '${QUERY_ID}' AND type = 'QueryFinish' AND current_database = currentDatabase()
ORDER BY event_time_microseconds DESC
LIMIT 1
"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dl_single"
rm -rf "$TABLE_PATH"
