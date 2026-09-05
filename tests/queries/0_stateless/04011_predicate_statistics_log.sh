#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="test_pred_ext_${CLICKHOUSE_DATABASE}"
TABLE_MC="${TABLE}_mc"
TABLE_OFF="${TABLE}_disabled"

ENABLE_STATS="SET predicate_statistics_sample_rate = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"
ENABLE_STATS_MULTI_STEP="$ENABLE_STATS, enable_multiple_prewhere_read_steps = 1"

Q1="${TABLE}_q1"
Q2="${TABLE}_q2"
Q3="${TABLE}_q3"
Q4="${TABLE}_q4"
Q5="${TABLE}_q5"
Q6="${TABLE}_q6"

$CLICKHOUSE_CLIENT -m --query "
$ENABLE_STATS;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE (
    id UInt64,
    status String,
    category String,
    score Float64,
    tag Nullable(String)
) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO $TABLE SELECT
    number,
    if(number % 10 = 0, 'active', 'inactive'),
    if(number % 3 = 0, 'cat_a', 'cat_b'),
    number * 0.1,
    if(number % 5 = 0, NULL, 'val')
FROM numbers(100000);
"

# Q1: simple equality → ~10% selectivity, non-empty predicate_expression
$CLICKHOUSE_CLIENT --query_id="$Q1" --query "$ENABLE_STATS; SELECT * FROM $TABLE WHERE status = 'active' FORMAT Null"

# Q2: 100% selectivity (all rows pass)
$CLICKHOUSE_CLIENT --query_id="$Q2" --query "$ENABLE_STATS; SELECT * FROM $TABLE WHERE id >= 0 FORMAT Null"

# Q3: 0% selectivity (no rows pass)
$CLICKHOUSE_CLIENT --query_id="$Q3" --query "$ENABLE_STATS; SELECT * FROM $TABLE WHERE status = 'nonexistent' FORMAT Null"

# Q4: multi-column predicate is still logged (expression captures it)
$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS $TABLE_MC;
CREATE TABLE $TABLE_MC (id UInt64, score Float64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO $TABLE_MC SELECT number, number * 0.5 FROM numbers(1000);
"
$CLICKHOUSE_CLIENT --query_id="$Q4" --query "$ENABLE_STATS; SELECT * FROM $TABLE_MC PREWHERE id > score FORMAT Null"

# Q5: sample_rate = 0 → nothing logged
$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS $TABLE_OFF;
CREATE TABLE $TABLE_OFF (id UInt64, status String) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO $TABLE_OFF SELECT number, 'x' FROM numbers(1000);
"
$CLICKHOUSE_CLIENT --query_id="$Q5" --query "SET predicate_statistics_sample_rate = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1; SELECT * FROM $TABLE_OFF WHERE status = 'x' FORMAT Null"

# Q6: conjunction split across multiple prewhere steps — total_selectivity is consistent and low
$CLICKHOUSE_CLIENT --query_id="$Q6" --query "$ENABLE_STATS_MULTI_STEP; SELECT * FROM $TABLE WHERE status = 'active' AND category = 'cat_a' FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS predicate_statistics_log"

# Q1: equality ~10% selectivity, predicate_expression is not empty
echo '--- q1 equality selectivity ~10% ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    min(length(predicate_expression)) > 0 AS has_expr,
    sum(input_rows) > 0 AS has_input,
    sum(passed_rows) > 0 AS has_passed,
    round(sum(passed_rows) / sum(input_rows), 1) AS sel
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND query_id = '$Q1' AND predicate_expression != '';
"

# Q2: 100% selectivity
echo '--- q2 100% selectivity ---'
$CLICKHOUSE_CLIENT --query "
SELECT round(max(filter_selectivity), 1) AS sel
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND query_id = '$Q2' AND predicate_expression != '';
"

# Q3: 0% selectivity
echo '--- q3 0% selectivity ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    sum(input_rows) > 0 AS has_input,
    sum(passed_rows) = 0 AS zero_passed
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND query_id = '$Q3' AND predicate_expression != '';
"

# Q4: multi-column predicate logged with non-empty predicate_expression
echo '--- q4 multi-column logged ---'
$CLICKHOUSE_CLIENT --query "
SELECT count() > 0 AS logged
FROM system.predicate_statistics_log
WHERE table = '$TABLE_MC' AND query_id = '$Q4' AND predicate_expression != '';
"

# Q5: disabled — nothing logged
echo '--- q5 disabled ---'
$CLICKHOUSE_CLIENT --query "
SELECT count() = 0 AS nothing_logged
FROM system.predicate_statistics_log
WHERE table = '$TABLE_OFF' AND query_id = '$Q5';
"

# Q6: conjunction — total_selectivity consistent across step rows and low
echo '--- q6 conjunction total_selectivity ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    round(min(total_selectivity), 2) = round(max(total_selectivity), 2) AS same_whole_sel,
    min(total_selectivity) < 0.1 AS selective
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND query_id = '$Q6' AND predicate_expression != '';
"

# Selectivity bounds
echo '--- selectivity bounds ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    min(filter_selectivity) >= 0 AS min_ok,
    max(filter_selectivity) <= 1 AS max_ok,
    min(total_selectivity) >= 0 AS total_min_ok,
    max(total_selectivity) <= 1 AS total_max_ok
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND predicate_expression != '';
"

# passed_rows <= input_rows invariant
echo '--- passed <= input ---'
$CLICKHOUSE_CLIENT --query "
SELECT count() = 0 AS ok
FROM system.predicate_statistics_log
WHERE table = '$TABLE' AND passed_rows > input_rows;
"

$CLICKHOUSE_CLIENT --query "DROP TABLE $TABLE"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE_OFF"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE_MC"
