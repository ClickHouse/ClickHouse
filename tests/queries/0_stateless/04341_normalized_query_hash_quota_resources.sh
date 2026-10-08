#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `KEYED BY normalized_query_hash` quota must give each distinct query pattern its own bucket for
# every resource, not only for the query-count counters. Previously the resource counters fed by the
# pipeline callbacks (`read_rows`/`read_bytes` from the read progress, `result_rows`/`result_bytes`/
# `execution_time` from the limits transform, `written_bytes` from the counting transform) and the
# `errors` counter (fed from the exception callbacks) were accounted against the shared per-user
# intervals, so they behaved as a single global bucket regardless of the query pattern. This test
# checks that two structurally different queries (which have different normalized hashes) get
# independent buckets for `read_rows`, `result_rows`, `written_bytes` and `errors`.
#
# Quotas and users are server-global, so the names are suffixed with the unique database name to keep
# the test isolated when it runs in parallel with itself (e.g. in the flaky check).

user="u_04341_${CLICKHOUSE_DATABASE}"
quota="q_04341_${CLICKHOUSE_DATABASE}"

# shellcheck disable=SC2086  # CLICKHOUSE_CLIENT must be word-split.
check_exceeded() { ${CLICKHOUSE_CLIENT} --user "${user}" --send_logs_level=none -q "$1" 2>&1 | grep -m1 -o QUOTA_EXCEEDED || echo allowed; }

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04341"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, INSERT ON *.* TO ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_04341 (x UInt64) ENGINE = Memory"

echo "=== read_rows is bucketed per normalized query hash ==="
# numbers(7) reads 7 rows; the literal is normalized away, so numbers(7) and numbers(13) share a hash.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX read_rows = 10 TO ${user}"

echo "--- pattern A, first query (7 read rows) is within the per-pattern limit ---"
check_exceeded "SELECT sum(number) FROM numbers(7) FORMAT Null"
echo "--- pattern A, second query (14 > 10) exceeds the limit of its own bucket ---"
check_exceeded "SELECT sum(number) FROM numbers(13) FORMAT Null"
echo "--- pattern B (a different query pattern) has its own bucket and is still allowed ---"
check_exceeded "SELECT avg(number) FROM numbers(7) FORMAT Null"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${quota}"

echo "=== result_rows is bucketed per normalized query hash ==="
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX result_rows = 10 TO ${user}"

echo "--- pattern A, first query (7 result rows) is within the per-pattern limit ---"
check_exceeded "SELECT number FROM numbers(7) FORMAT Null"
echo "--- pattern A, second query (14 > 10) exceeds the limit of its own bucket ---"
check_exceeded "SELECT number FROM numbers(13) FORMAT Null"
echo "--- pattern B (a different query pattern) has its own bucket and is still allowed ---"
check_exceeded "SELECT number * 2 FROM numbers(7) FORMAT Null"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"

echo "=== written_bytes is bucketed per normalized query hash ==="
# Each inserted UInt64 row is 8 bytes (the counting transform counts the in-memory block size). The
# literal in numbers() is normalized away, so numbers(7)/numbers(13) share a hash; `number * 2` is a
# structurally different pattern with its own bucket.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX written_bytes = 80 TO ${user}"

echo "--- pattern A, first insert (7 rows = 56 bytes) is within the per-pattern limit ---"
check_exceeded "INSERT INTO t_04341 SELECT number FROM numbers(7)"
echo "--- pattern A, second insert (cumulative 160 > 80) exceeds the limit of its own bucket ---"
check_exceeded "INSERT INTO t_04341 SELECT number FROM numbers(13)"
echo "--- pattern B (a different query pattern) has its own bucket and is still allowed ---"
check_exceeded "INSERT INTO t_04341 SELECT number * 2 FROM numbers(7)"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"

echo "=== errors are bucketed per normalized query hash ==="
# `errors` is fed from the exception callbacks, not the pulling pipeline. The limit is enforced as a
# pre-check at the start of each query: a query is rejected with QUOTA_EXCEEDED once its bucket already
# exceeds the limit. Two failing queries of pattern A (the throwIf literals are normalized away, so
# they share a hash) raise its bucket to 2 > 1; the third query of the same pattern is then rejected
# up-front, before throwIf runs. Pattern B is a structurally different (and successful) query, so its
# error bucket stays empty and independent.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX errors = 1 TO ${user}"

${CLICKHOUSE_CLIENT} --user "${user}" --send_logs_level=none -q "SELECT throwIf(1, 'boom') FROM numbers(7)" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} --user "${user}" --send_logs_level=none -q "SELECT throwIf(1, 'boom') FROM numbers(13)" >/dev/null 2>&1

echo "--- pattern A: the error bucket is exceeded, so the next query of the same pattern is rejected up-front ---"
check_exceeded "SELECT throwIf(1, 'boom') FROM numbers(5)"
echo "--- pattern B (a different query pattern) has its own error bucket and is still allowed ---"
check_exceeded "SELECT count() FROM numbers(7) FORMAT Null"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04341"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
