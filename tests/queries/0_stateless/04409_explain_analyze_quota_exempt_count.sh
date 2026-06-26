#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# EXPLAIN ANALYZE executes the inner SELECT, so it must follow the same query-count quota
# (QUERY_SELECTS / QUERIES) rules as running that SELECT directly. Quota-exempt sources such as
# system.one set ignore_quota during planning, so neither a plain SELECT nor EXPLAIN ANALYZE over
# them may consume the query-count quota. A non-exempt source (e.g. numbers()) must still be charged.

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q04409"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u04409"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r04409"

${CLICKHOUSE_CLIENT} -q "CREATE ROLE r04409"
${CLICKHOUSE_CLIENT} -q "CREATE USER u04409"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO r04409"
${CLICKHOUSE_CLIENT} -q "GRANT r04409 TO u04409"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA q04409 KEYED BY user_name FOR INTERVAL 100 YEAR MAX query_selects = 1 TO r04409"

for _ in {1..100}; do
    ${CLICKHOUSE_CLIENT} --user u04409 --send_logs_level=none -q "EXPLAIN ANALYZE SELECT dummy FROM system.one" 2>&1 | grep -o 'QUOTA_EXCEEDED'
    [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.quotas_usage WHERE quota_name = 'q04409'")" -gt 0 ] && break
    sleep 0.3
done
echo "query_selects after exempt source:"
${CLICKHOUSE_CLIENT} -q "SELECT sum(query_selects) FROM system.quotas_usage WHERE quota_name = 'q04409'"

# Non-exempt source: EXPLAIN ANALYZE must consume query_selects, so the second one is rejected.
${CLICKHOUSE_CLIENT} --user u04409 --send_logs_level=none -q "EXPLAIN ANALYZE SELECT number FROM numbers(1)" > /dev/null 2>&1
echo "second non-exempt EXPLAIN ANALYZE:"
${CLICKHOUSE_CLIENT} --user u04409 --send_logs_level=none -q "EXPLAIN ANALYZE SELECT number FROM numbers(1)" 2>&1 | grep -o 'QUOTA_EXCEEDED'

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q04409"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u04409"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r04409"
