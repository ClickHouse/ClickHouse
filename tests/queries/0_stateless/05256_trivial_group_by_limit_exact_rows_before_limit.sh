#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `exact_rows_before_limit` promises the exact number of groups in `rows_before_limit_at_least`.
# The trivial `GROUP BY ... LIMIT` optimization caps the aggregation at `LIMIT + OFFSET` groups
# (the kept-keys cutoff with aggregate functions, `max_rows_to_group_by` without them), which
# would report at most that many, so it must stay off. The `GROUP BY` top-K heap has the same
# guard; it is disabled here so that only the cutoff could apply.

SETTINGS="optimize_trivial_group_by_limit_query = 1, enable_group_by_top_k_optimization = 0, exact_rows_before_limit = 1, max_threads = 4, max_block_size = 1000, enable_parallel_replicas = 0"

for query in \
    "SELECT number AS k, count() FROM numbers_mt(1000000) GROUP BY k LIMIT 5" \
    "SELECT number AS k, count() FROM numbers_mt(1000000) GROUP BY k LIMIT 5 OFFSET 3" \
    "SELECT number AS k FROM numbers_mt(1000000) GROUP BY k LIMIT 5"
do
    echo "$query"
    $CLICKHOUSE_CLIENT --query "$query FORMAT JSON SETTINGS $SETTINGS" | grep -o '"rows_before_limit_at_least": [0-9]*'
done
