#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/110342
# A multi-command ALTER (UPDATE ..., DELETE ...) on an Iceberg table used to
# trip chassert(commands.size() == 1) in Iceberg::writeDataFiles, aborting
# debug/sanitizer builds. It must now be rejected with NOT_IMPLEMENTED.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 Int32) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} SELECT number, number FROM numbers(10)"

# grep the client exception's error code; --send_logs_level (injected by the
# test runner) can also echo the server-side error, so keep only the first hit.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE ${TABLE} UPDATE c1 = c1 + 100 WHERE c0 < 3, DELETE WHERE c0 > 7
" 2>&1 | grep -oF "NOT_IMPLEMENTED" | head -n1

# The rejected ALTER must have no side effects: all 10 rows must survive with
# their original c1 values (no partial UPDATE of c0 < 3, no partial DELETE of c0 > 7).
${CLICKHOUSE_CLIENT} --query "SELECT c0, c1 FROM ${TABLE} ORDER BY c0"

# A single-command mutation still works.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE c0 > 7"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
