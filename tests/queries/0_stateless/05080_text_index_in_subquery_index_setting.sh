#!/usr/bin/env bash

# `use_index_for_in_with_subqueries = 0` forbids using an index for a set on the right of `IN`.
# `FutureSetFromSubquery::buildOrderedSetInplace` enforces that by refusing to build, which is what
# the map-key and JSON-path helpers used to rely on. Refusing to build is not the same as refusing to
# use, though: `ReadFromMergeTree::applyFilters` builds PREWHERE sets unordered precisely when the
# ordered build declines, so a read step analyzed after that finds the set ready. It takes a second
# read step to see it - the first memoizes its indexes before the sets are built - which lazy FINAL
# provides by cloning the reading step.
#
# The analyzer is requested explicitly, as every lazy FINAL test does: without it `optimizeLazyFinal`
# does not run and there is no second read step to analyze.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

settings="--enable_analyzer=1"

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_text_index_in_setting;
    CREATE TABLE t_text_index_in_setting
    (
        id UInt64,
        m Map(String, String),
        version UInt64,
        INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'splitByNonAlpha')
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY id
    SETTINGS index_granularity = 8, min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;

    SYSTEM STOP MERGES t_text_index_in_setting;
    -- Two parts over the same keys, so every part intersects and lazy FINAL clones the reading step.
    INSERT INTO t_text_index_in_setting
        SELECT number, if(number % 2 = 0, map('k', 'val' || toString(number)), map('other', 'z')), 1 FROM numbers(400);
    INSERT INTO t_text_index_in_setting
        SELECT number, if(number % 2 = 0, map('k', 'val' || toString(number)), map('other', 'z')), 2 FROM numbers(400);
"

function index_consulted()
{
    $CLICKHOUSE_CLIENT $settings -q "
        SELECT count() FROM t_text_index_in_setting FINAL
        PREWHERE m['k'] IN (SELECT arrayJoin(['val0', 'val2', 'val4']))
        SETTINGS use_index_for_in_with_subqueries = $1, query_plan_optimize_lazy_final = 1,
                 min_filtered_ratio_for_lazy_final = 0
    " --send_logs_level='debug' 2>&1 \
        | grep -c -F 'Index `idx_keys` has dropped' \
        | sed 's/^0$/no/; s/^[1-9][0-9]*$/yes/'
}

echo "-- use_index_for_in_with_subqueries = 1: the index is used"
index_consulted 1

echo "-- use_index_for_in_with_subqueries = 0: the index is left alone"
index_consulted 0

echo "-- and the answer is the same either way"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM t_text_index_in_setting FINAL PREWHERE m['k'] IN (SELECT arrayJoin(['val0', 'val2', 'val4'])) SETTINGS use_index_for_in_with_subqueries = 1"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM t_text_index_in_setting FINAL PREWHERE m['k'] IN (SELECT arrayJoin(['val0', 'val2', 'val4'])) SETTINGS use_index_for_in_with_subqueries = 0"

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE t_text_index_in_setting"
