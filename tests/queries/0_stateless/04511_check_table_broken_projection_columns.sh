#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-object-storage

# A projection part that fails to load before its columns are set (here: corrupted
# serialization.json) has an empty column list. CHECK TABLE used to compare the projection's
# on-disk columns.txt against that empty list and report a misleading "Columns doesn't match ...
# Expected: 0 columns" error, hiding the real corruption. It must report the actual problem.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_broken_proj_check SYNC"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_broken_proj_check (id UInt64, v UInt64, PROJECTION p1 (SELECT v, count() GROUP BY v))
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_full_part_storage = 0"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_broken_proj_check SELECT number, number % 10 FROM numbers(1000)"

DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_broken_proj_check' AND active")

${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_broken_proj_check"
printf 'garbage' > "${DATA_PATH}p1.proj/serialization.json"
# The attach legitimately logs the broken projection at error level; do not let the
# server logs reach stderr under randomized send_logs_level.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "ATTACH TABLE t_broken_proj_check"

RESULT=$(${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "CHECK TABLE t_broken_proj_check SETTINGS check_query_single_value_result = 0 FORMAT TSV")

echo -n "part check passed: "
echo "$RESULT" | cut -f2
echo -n "reports the misleading columns mismatch: "
echo "$RESULT" | grep -c "Columns doesn.t match" || true
echo -n "reports the real corruption in serialization.json: "
echo "$RESULT" | grep -c "serialization.json" || true

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_broken_proj_check SYNC"

# The same, but for a projection part that legitimately lags behind the current projection schema:
# after ALTER ADD COLUMN, a SELECT * projection includes the new column in its metadata while the
# existing projection parts do not store it (and no mutation is pending, so nothing rewrites them).
# The expected columns must come from the part itself, not from the current projection metadata.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_broken_proj_check_lagging SYNC"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_broken_proj_check_lagging (id UInt64, v UInt16, PROJECTION p1 (SELECT * ORDER BY v))
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_full_part_storage = 0"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_broken_proj_check_lagging SELECT number, number % 10 FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_broken_proj_check_lagging ADD COLUMN extra UInt8"

DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_broken_proj_check_lagging' AND active")

${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_broken_proj_check_lagging"
printf 'garbage' > "${DATA_PATH}p1.proj/serialization.json"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "ATTACH TABLE t_broken_proj_check_lagging"

RESULT=$(${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "CHECK TABLE t_broken_proj_check_lagging SETTINGS check_query_single_value_result = 0 FORMAT TSV")

echo -n "lagging part check passed: "
echo "$RESULT" | cut -f2
echo -n "lagging part reports the misleading columns mismatch: "
echo "$RESULT" | grep -c "Columns doesn.t match" || true
echo -n "lagging part reports the real corruption in serialization.json: "
echo "$RESULT" | grep -c "serialization.json" || true

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_broken_proj_check_lagging SYNC"
