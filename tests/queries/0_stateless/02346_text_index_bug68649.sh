#!/usr/bin/env bash
# Tags: no-parallel-replicas

# A `__text_index_*` virtual column has no stored size, and the PREWHERE score used to fall back to a
# row count for such columns. A row count is not comparable with bytes per rejected row, so every
# physical-column predicate won and ran first, on all rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

MY_CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT \
    --allow_experimental_full_text_index 1 \
    --enable_analyzer 1 \
    --use_statistics 1 \
    --materialize_statistics_on_insert 1 \
    --optimize_move_to_prewhere 1 \
    --query_plan_optimize_prewhere 1 \
    --allow_reorder_prewhere_conditions 1 \
    --use_skip_indexes 1 \
    --use_skip_indexes_on_data_read 1 \
    --query_plan_direct_read_from_text_index 1"

# Wide parts give the physical columns a real size; the tdigest on `v` activates the estimator.
$MY_CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS tab;

    CREATE TABLE tab
    (
        id UInt64,
        v UInt64 STATISTICS(tdigest),
        text String,
        INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha')
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, auto_statistics_types = '';

    INSERT INTO tab
    SELECT
        number,
        number % 1000,
        concat('alpha beta gamma ', toString(number), ' ', repeat(hex(sipHash64(number)), 8))
    FROM numbers(20000)
    SETTINGS max_insert_threads = 1;
"

# A query with a conjunction as WHERE filter
# - `hasToken` uses exact direct read, so it becomes a read of a virtual column.
# - The `=` filter is against a non-constant value, so it stays raw scan
QUERY="SELECT count() FROM tab WHERE hasToken(text, 'alpha') AND text = concat('x', toString(v))"

echo '-- no row matches the equality'
$MY_CLICKHOUSE_CLIENT --query "$QUERY"

# Conditions are logged in scheduling order.
# Expect that `hasToken` (virtual column __text_index_) runs first in PREWHERE
echo '-- PREWHERE scheduling order'
MY_CLICKHOUSE_CLIENT_TEST_LOGS=$(echo "$MY_CLICKHOUSE_CLIENT" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=test/g")
$MY_CLICKHOUSE_CLIENT_TEST_LOGS --query "$QUERY" 2>&1 >/dev/null \
    | grep -F 'moved to PREWHERE' \
    | awk '/__text_index_/ { print "text index virtual column"; next } { print "text column predicate" }' \
    | head -n 2

$MY_CLICKHOUSE_CLIENT --query "DROP TABLE tab"
