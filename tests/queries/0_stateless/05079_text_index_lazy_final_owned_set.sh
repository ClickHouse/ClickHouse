#!/usr/bin/env bash

# `optimizeLazyFinal` builds a set of primary key values and hands it to `FunctionIn` wrapped in a
# `FutureSetFromStorage`, even though no table backs it. `LazyFinalKeyAnalysisTransform` then runs
# index analysis on `<primary key expression> IN <that set>`, so with `ORDER BY m['k']` and a
# `mapKeys` text index the analysis reaches the same code that refuses a set living in a table.
# That set is the query's own and cannot change under it, so the index must still be consulted here;
# rejecting every `FutureSetFromStorage` would silently drop the index for such a `FINAL` query.
#
# Asserted from the transform's own trace, the way the other lazy FINAL tests assert theirs. Only the
# presence of the line is checked: how many granules it drops depends on the granularity the test
# randomizer picks, while being consulted at all is the property under test.
#
# The analyzer is requested explicitly, as every other lazy FINAL test does: without it
# `optimizeLazyFinal` does not run at all, so `LazyFinalKeyAnalysisTransform` never reaches index
# analysis and the assertion below has nothing to observe.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

settings="--enable_analyzer=1"

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_lazy_final_text_index;
    CREATE TABLE t_lazy_final_text_index
    (
        m Map(String, String),
        version UInt64,
        status String,
        INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'splitByNonAlpha')
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY m['k']
    SETTINGS index_granularity = 8, min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;

    SYSTEM STOP MERGES t_lazy_final_text_index;
    -- Two parts over the same keys, so every part intersects and lazy FINAL has work to do.
    -- Every row carries the key, which keeps the default value out of the set of key values below
    -- and so keeps the index usable at all.
    INSERT INTO t_lazy_final_text_index
        SELECT map('k', 'key' || toString(number)), 1, if(number < 100, 'target', 'other') FROM numbers(400);
    INSERT INTO t_lazy_final_text_index
        SELECT map('k', 'key' || toString(number)), 2, if(number < 100, 'target', 'other') FROM numbers(400);
"

echo "-- correctness does not depend on lazy FINAL"
$CLICKHOUSE_CLIENT $settings -q "SELECT count(), uniqExact(m['k']) FROM t_lazy_final_text_index FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 0"
$CLICKHOUSE_CLIENT $settings -q "SELECT count(), uniqExact(m['k']) FROM t_lazy_final_text_index FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0, max_bytes_for_lazy_final = 0"

echo "-- the text index is consulted for the set the query owns"
# The set of key values must not hit a byte limit: a truncated set disables lazy FINAL before index
# analysis runs (0 means unlimited), and the randomizer picks a 1-byte limit.
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_text_index FINAL WHERE status = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0, max_bytes_for_lazy_final = 0
" --send_logs_level='debug' 2>&1 \
    | grep -c -F 'LazyFinalKeyAnalysisTransform: Index `idx_keys` has dropped' \
    | sed 's/^0$/no/; s/^[1-9][0-9]*$/yes/'

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE t_lazy_final_text_index"
