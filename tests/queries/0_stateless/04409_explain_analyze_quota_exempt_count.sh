#!/usr/bin/env bash
# Tags: no-parallel
# Reason: modifies quotas and creates users

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# EXPLAIN ANALYZE executes the inner SELECT, so it must follow the same query-count quota
# (QUERY_SELECTS) rules as running that SELECT directly. Quota-exempt sources such as
# system.one set ignore_quota during planning, so EXPLAIN ANALYZE over them must not consume
# the query-count quota, while a non-exempt source (e.g. numbers()) must still be charged.

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q04409"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u04409"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r04409"

${CLICKHOUSE_CLIENT} -q "CREATE ROLE r04409"
${CLICKHOUSE_CLIENT} -q "CREATE USER u04409"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO r04409"
${CLICKHOUSE_CLIENT} -q "GRANT r04409 TO u04409"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA q04409 KEYED BY user_name FOR INTERVAL 100 YEAR MAX query_selects = 1 TO r04409"

# Exempt source: EXPLAIN ANALYZE over system.one must not consume query_selects. Running it
# twice does not exhaust the MAX query_selects = 1 budget, so the first non-exempt query below
# still succeeds - if the exempt queries had been charged, that first one would already be rejected.
${CLICKHOUSE_CLIENT} --user u04409 --send_logs_level=none -q "EXPLAIN ANALYZE SELECT dummy FROM system.one" > /dev/null 2>&1
${CLICKHOUSE_CLIENT} --user u04409 --send_logs_level=none -q "EXPLAIN ANALYZE SELECT dummy FROM system.one" > /dev/null 2>&1

# Non-exempt source: EXPLAIN ANALYZE must consume query_selects, so the first succeeds and the
# second is rejected.
echo "first non-exempt EXPLAIN ANALYZE:"
${CLICKHOUSE_CLIENT} --user u04409 --send_logs_level=none -q "EXPLAIN ANALYZE SELECT number FROM numbers(1)" 2>&1 | grep -o 'QUOTA_EXCEEDED'
echo "non-exempt query exit code: ${PIPESTATUS[0]}"
echo "second non-exempt EXPLAIN ANALYZE:"
${CLICKHOUSE_CLIENT} --user u04409 --send_logs_level=none -q "EXPLAIN ANALYZE SELECT number FROM numbers(1)" 2>&1 | grep -o 'QUOTA_EXCEEDED'

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q04409"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u04409"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r04409"
