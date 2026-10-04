#!/usr/bin/env bash
# The aggregation of a query with a nested SETTINGS clause uses the settings in effect for that query,
# after the settings constraints are applied, not the clause as written.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A settings profile is server-global, so its name carries the test database to keep this test safe
# against a concurrent copy of itself.
PROFILE="profile_05316_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --multiquery <<'EOF'
SELECT 'a nested max_threads reaches the aggregation';
SELECT max(toUInt64(n)) FROM
(
    EXPLAIN PIPELINE SELECT count() FROM (SELECT number FROM numbers_mt(100000) GROUP BY number SETTINGS max_threads = 23, max_threads_min_free_memory_per_thread = 0)
    SETTINGS max_threads = 2
)
ARRAY JOIN extractAll(explain, '[0-9]+') AS n;

SELECT 'a nested clause dropped in readonly mode does not reach the aggregation';
SELECT max(toUInt64(n)) <= 2 FROM
(
    EXPLAIN PIPELINE SELECT count() FROM (SELECT number FROM numbers_mt(100000) GROUP BY number SETTINGS max_threads = 23, max_threads_min_free_memory_per_thread = 0)
    SETTINGS max_threads = 2, readonly = 1
)
ARRAY JOIN extractAll(explain, '[0-9]+') AS n;

SELECT 'GROUP BY LIMIT stops at LIMIT keys';
SELECT count(), sum(c) > 0 FROM (SELECT toUInt64(number % 1000) AS k, count() AS c FROM numbers(10000) GROUP BY k LIMIT 10 SETTINGS max_rows_to_group_by = 100);
EOF

echo 'a nested group_by_overflow_mode equal to the inherited value is still respected'
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(c) > 0 FROM (SELECT toUInt64(number % 1000) AS k, count() AS c FROM numbers(10000) GROUP BY k LIMIT 10 SETTINGS max_rows_to_group_by = 100, group_by_overflow_mode = 'throw')" 2>&1 | grep -m1 -c -F '(TOO_MANY_ROWS)'

echo 'a nested max_threads above a MAX constraint is clamped into it for the aggregation'
${CLICKHOUSE_CLIENT} --multiquery --query "
DROP SETTINGS PROFILE IF EXISTS ${PROFILE};
CREATE SETTINGS PROFILE ${PROFILE} SETTINGS max_threads MAX 64;
SET profile = '${PROFILE}';
SELECT max(toUInt64(n)) FROM
(
    EXPLAIN PIPELINE SELECT count() FROM (SELECT number FROM numbers_mt(100000) GROUP BY number SETTINGS max_threads = 100, max_threads_min_free_memory_per_thread = 0)
    SETTINGS max_threads = 2
)
ARRAY JOIN extractAll(explain, '[0-9]+') AS n;
"

${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE ${PROFILE}"
