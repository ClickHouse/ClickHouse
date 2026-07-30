#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# Tag no-parallel: toggles a server-global failpoint.
# Tag no-replicated-database: hypothetical indexes are session-scoped and not replicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_statistics=1 --materialize_statistics_on_insert=1 --allow_experimental_statistics=1 --allow_statistics_optimize=1"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT merge_tree_load_statistics_throw" >/dev/null 2>&1 ||:
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_full" >/dev/null 2>&1 ||:
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_packed" >/dev/null 2>&1 ||:
}

expect_error()
{
    local expected_error="$1"
    local query="$2"
    local output

    if output=$(${CLICKHOUSE_CLIENT} --multiquery --query "$query" 2>&1); then
        echo "Expected ${expected_error}, but query succeeded" >&2
        return 1
    fi

    if ! printf '%s\n' "$output" | grep -qF "$expected_error"; then
        printf '%s\n' "$output" >&2
        return 1
    fi
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE t_full (a UInt64 STATISTICS(basic), b UInt64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0,
             min_bytes_for_full_part_storage = 0,
             max_bytes_to_merge_at_max_space_in_pool = 1,
             refresh_statistics_interval = 0;

    CREATE TABLE t_packed (a UInt64 STATISTICS(basic), b UInt64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0,
             min_bytes_for_full_part_storage = '1G',
             max_bytes_to_merge_at_max_space_in_pool = 1,
             refresh_statistics_interval = 0;

    INSERT INTO t_full SELECT number, number FROM numbers(1000);
    INSERT INTO t_full SELECT number + 1000000, number FROM numbers(1000);
    INSERT INTO t_packed SELECT number, number FROM numbers(1000);
    INSERT INTO t_packed SELECT number + 1000000, number FROM numbers(1000);

    DETACH TABLE t_full;
    ATTACH TABLE t_full;
    DETACH TABLE t_packed;
    ATTACH TABLE t_packed;
"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT merge_tree_load_statistics_throw"

# Both statistics storage representations must propagate a deserialize failure.
expect_error "CANNOT_READ_ALL_DATA" "
    SELECT count() FROM t_full WHERE a > 500000
    SETTINGS use_statistics_for_part_pruning = 1, use_statistics_cache = 1
"
expect_error "CANNOT_READ_ALL_DATA" "
    SELECT count() FROM t_packed WHERE a > 500000
    SETTINGS use_statistics_for_part_pruning = 1, use_statistics_cache = 0
"

# Query planning must propagate the same load failure.
expect_error "CANNOT_READ_ALL_DATA" "
    SELECT sum(b) FROM t_full WHERE a > 500000 AND b < 500
    SETTINGS use_statistics = 1, use_statistics_cache = 0,
             use_statistics_for_part_pruning = 0,
             enable_analyzer = 0, optimize_move_to_prewhere = 1
"

expect_error "CANNOT_READ_ALL_DATA" "
    CREATE HYPOTHETICAL INDEX idx_a ON t_full (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF empirical = 0 SELECT * FROM t_full WHERE a > 500000
    SETTINGS use_statistics_for_part_pruning = 0;
"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT merge_tree_load_statistics_throw"

# After the failure, both representations must load valid statistics and prune all parts.
for table in t_full t_packed
do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM ${table} WHERE a > 2000000
        SETTINGS use_statistics_for_part_pruning = 1,
                 use_statistics_cache = 1,
                 log_comment = '04209_statistics_retry_load_${table}'
    "
done

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['SelectedParts']
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment IN ('04209_statistics_retry_load_t_full', '04209_statistics_retry_load_t_packed')
      AND type = 'QueryFinish'
    ORDER BY log_comment
"
