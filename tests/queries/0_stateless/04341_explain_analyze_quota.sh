#!/usr/bin/env bash
# Tags: no-parallel
# Reason: modifies quotas and creates users

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# EXPLAIN ANALYZE executes the inner SELECT, so it must be charged against the
# select-query quota (QUERY_SELECTS) just like a normal SELECT. Plain EXPLAIN does
# not execute the inner query and must stay counted as a generic query only.

# Note: queries like SELECT 1 use system.one, which bypasses quota checks.
# We must use queries with an explicit FROM clause to a non-exempt table.

${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r04341"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u04341"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q04341_selects"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q04341_plan"

${CLICKHOUSE_CLIENT} -q "CREATE ROLE r04341"
${CLICKHOUSE_CLIENT} -q "CREATE USER u04341"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO r04341"
${CLICKHOUSE_CLIENT} -q "GRANT r04341 TO u04341"

# ============================================================
# EXPLAIN ANALYZE consumes the select-query quota.
# ============================================================
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA q04341_selects FOR INTERVAL 100 YEAR MAX query_selects = 1 TO r04341"

${CLICKHOUSE_CLIENT} --user u04341 --send_logs_level=none -q "EXPLAIN ANALYZE SELECT number FROM numbers(1)" > /dev/null 2>&1
${CLICKHOUSE_CLIENT} --user u04341 --send_logs_level=none -q "EXPLAIN ANALYZE SELECT number FROM numbers(1)" 2>&1 | grep -o 'QUOTA_EXCEEDED'

${CLICKHOUSE_CLIENT} -q "DROP QUOTA q04341_selects"

${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r04341"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u04341"
