#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped and not replicated

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `EXPLAIN WHATIF` empirical scan reads data through the read-progress callback, so its `read_rows`
# are accounted against the query's quota. Under a `KEYED BY normalized_query_hash` quota each distinct
# `EXPLAIN WHATIF` query pattern must get its own bucket, just like a real query. Previously the scan's
# `QueryPipeline` never received the normalized query hash, so every `EXPLAIN WHATIF` pattern shared the
# single hash-`0` bucket; once one pattern exhausted it, a structurally different pattern was rejected
# too. This test checks that two structurally different `EXPLAIN WHATIF` patterns get independent
# `read_rows` buckets.
#
# Quotas and users are server-global, so the names are suffixed with the unique database name to keep
# the test isolated when it runs in parallel with itself (e.g. in the flaky check).

user="u_04490_${CLICKHOUSE_DATABASE}"
quota="q_04490_${CLICKHOUSE_DATABASE}"

# shellcheck disable=SC2086  # CLICKHOUSE_CLIENT must be word-split.
# The hypothetical index is session-scoped, so it is (re)created in the same client call as the
# `EXPLAIN WHATIF` that uses it.
whatif() { ${CLICKHOUSE_CLIENT} --user "${user}" --send_logs_level=none -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_04490 (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_04490 WHERE $1;
" 2>&1 | grep -m1 -o QUOTA_EXCEEDED || echo allowed; }

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04490"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON *.* TO ${user}"

# One part of 1000 rows in 10 granules. `b` is not in the primary key, so a predicate on `b` does not
# prune granules and the empirical scan reads the whole part (1000 rows) for every pattern.
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t_04490 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_04490 SELECT number, number FROM numbers(1000);
"

# read_rows limit of 1500: one empirical scan (1000 rows) fits, a second one in the same bucket (2000)
# exceeds it.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX read_rows = 1500 TO ${user}"

echo "--- pattern A, first EXPLAIN WHATIF (1000 read rows) is within the per-pattern limit ---"
whatif "b = 42"
echo "--- pattern A, second EXPLAIN WHATIF (cumulative 2000 > 1500) exceeds the limit of its own bucket ---"
whatif "b = 42"
echo "--- pattern B (a structurally different EXPLAIN WHATIF) has its own bucket and is still allowed ---"
whatif "b >= 42"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04490"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
