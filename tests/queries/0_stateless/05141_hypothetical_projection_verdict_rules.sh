#!/usr/bin/env bash
# what EXPLAIN WHATIF refuses to estimate, and which settings it follows; the mark counts live in 05076

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# count() must reach the read step and the real projection must be allowed to win
PIN="optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, optimize_use_projections = 1, optimize_read_in_order = 1"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est; DROP TABLE IF EXISTS t_real;
    CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE t_real AS t_est;
    ALTER TABLE t_real ADD PROJECTION p_b (SELECT a, b, v ORDER BY b);
    INSERT INTO t_est SELECT number, number % 100, number FROM numbers(1000);
    INSERT INTO t_real SELECT number, number % 100, number FROM numbers(1000);
"

echo "--- not applicable cases ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_key ON t_est (SELECT a, b, v ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION p_agg ON t_est (SELECT b, sum(v) GROUP BY b);
    CREATE HYPOTHETICAL PROJECTION p_where ON t_est (SELECT a, b, v WHERE b < 50 ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION p_nocol ON t_est (SELECT a, b ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION p_skipidx ON t_est (SELECT a, b, v ORDER BY a) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1);
    EXPLAIN WHATIF SELECT sum(v) FROM t_est WHERE a = 500 SETTINGS ${PIN};
" | grep -E '^With|^\s+reason:' | awk '{$1=$1; print}'

# a key over a virtual column has no source among the columns the projection stores
echo "--- a key over a virtual column is not estimated ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_mx ON t_est (SELECT a, _part_offset ORDER BY _part_offset);
    EXPLAIN WHATIF SELECT a FROM t_est WHERE _part_offset = 7 SETTINGS ${PIN};
" 2>&1 | grep -E '^\s+reason:|Code:' | awk '{$1=$1; print}'

# an ALTER can re-point an ALIAS the definition selects, and the estimate must not read the new source
echo "--- retargeting an ALIAS the projection selects denies the estimate ---"
alias_user="u3_05141_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_alias;
    CREATE TABLE t_alias (a UInt64, b UInt64, d UInt64, c UInt64 ALIAS b + 1) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_alias (a, b, d) SELECT number, number % 100, number FROM numbers(1000);
    DROP USER IF EXISTS ${alias_user};
    CREATE USER ${alias_user} NOT IDENTIFIED;
    GRANT ALTER ADD PROJECTION ON ${CLICKHOUSE_DATABASE}.t_alias TO ${alias_user};
    GRANT SELECT(a, b, c) ON ${CLICKHOUSE_DATABASE}.t_alias TO ${alias_user};
"
# the store is per session, so the ALTER has to land between two statements of one HTTP session
alias_url="${CLICKHOUSE_URL}&user=${alias_user}&session_id=${CLICKHOUSE_DATABASE}_alias&session_timeout=600&optimize_respect_aliases=1"
${CLICKHOUSE_CURL} -sS "${alias_url}" --data-binary "CREATE HYPOTHETICAL PROJECTION p_al ON ${CLICKHOUSE_DATABASE}.t_alias (SELECT a, c ORDER BY a)"
${CLICKHOUSE_CURL} -sS "${alias_url}" --data-binary "EXPLAIN WHATIF SELECT a FROM ${CLICKHOUSE_DATABASE}.t_alias WHERE a = 500 SETTINGS ${PIN}" | grep -E '^\s+status:' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_alias MODIFY COLUMN c UInt64 ALIAS d + 1;"
${CLICKHOUSE_CURL} -sS "${alias_url}" --data-binary "EXPLAIN WHATIF SELECT a FROM ${CLICKHOUSE_DATABASE}.t_alias WHERE a = 500 SETTINGS ${PIN}" 2>&1 | grep -m1 -oE 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${alias_user}; DROP TABLE IF EXISTS t_alias;"

# the base read prunes a `_part_offset` predicate with its own offset condition, so a projection is
# not engaged at all: the granule count matches a table that has none, while `b = 42` still uses it
echo "--- a _part_offset predicate does not engage a projection ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_off; DROP TABLE IF EXISTS t_off_plain;
    CREATE TABLE t_off (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    CREATE TABLE t_off_plain AS t_off;
    ALTER TABLE t_off ADD PROJECTION p_b INDEX b TYPE basic;
    INSERT INTO t_off SELECT number, number % 100, number FROM numbers(1000);
    INSERT INTO t_off_plain SELECT number, number % 100, number FROM numbers(1000);
"
for w in "_part_offset = 7" "b = 42 AND _part_offset = 7" "b = 42"; do
    plan=$($CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT count() FROM t_off WHERE ${w} SETTINGS ${PIN}")
    plain=$($CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT count() FROM t_off_plain WHERE ${w} SETTINGS ${PIN}")
    echo "${w}: with $(echo "$plan" | grep -oE 'Granules: [0-9]+$' | head -1), without $(echo "$plain" | grep -oE 'Granules: [0-9]+$' | head -1), from projection $(echo "$plan" | grep -cE 'ReadFromMergeTree \(p_b\)')"
done
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_off; DROP TABLE IF EXISTS t_off_plain;"

# a commit_order projection is keyed on the commit order itself, which the scan cannot rebuild
echo "--- a commit_order projection is not estimated ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_co;
    CREATE TABLE t_co (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
                 allow_commit_order_projection = 1, enable_block_number_column = 1, enable_block_offset_column = 1;
    INSERT INTO t_co SELECT number, number % 100, number FROM numbers(1000);
    CREATE HYPOTHETICAL PROJECTION p_co ON t_co INDEX b TYPE commit_order;
    EXPLAIN WHATIF SELECT count() FROM t_co WHERE b = 42 SETTINGS ${PIN};
" | grep -E '^\s+reason:' | awk '{$1=$1; print}'
# the query form of the same thing gets the same reason
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_qco ON t_co
        (SELECT b, _block_number, _block_offset ORDER BY (_block_number, _block_offset));
    EXPLAIN WHATIF SELECT count() FROM t_co WHERE b = 42 SETTINGS ${PIN};
" | grep -E '^\s+reason:' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_co;"

echo "--- projections disabled by the query ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_est WHERE b = 42 SETTINGS optimize_trivial_count_query = 0, optimize_use_projections = 0;
" | grep -E '^\s+reason:' | awk '{$1=$1; print}'

echo "--- use_primary_key = 0 turns the projection's own pruning off, as in the optimizer ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_est WHERE b = 42 SETTINGS ${PIN}, use_primary_key = 0;
" | grep -E '^\s+reason:' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT count() FROM t_real WHERE b = 42 SETTINGS ${PIN}, use_primary_key = 0" \
    | grep -oE 'ReadFromMergeTree \(p_b\)' || echo "real: read from the base table"

echo "--- empirical = 0 scans nothing, and reports no marks it does not have ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF empirical = 0 SELECT count() FROM t_est WHERE b = 42 SETTINGS ${PIN};
" | sed -n '/^With/,$p' | grep -E '^\s+(status|source|empirical_status):|marks:' | awk '{$1=$1; print}'

echo "--- a forced projection must not fail the statement before the candidate is seen ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_est WHERE b = 42 SETTINGS ${PIN}, force_optimize_projection = 1;
" 2>&1 | grep -E '^\s+(status|verdict):|Code:' | awk '{$1=$1; print}'

# a projection sort key has no direction, so the synthetic part is sorted the only way one can be
echo "--- a query that reads no parts gets no verdict, the optimizer ignores projections then ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_pruned;
    CREATE TABLE t_pruned (d Date, a UInt64, b UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO t_pruned SELECT toDate('2020-01-01'), number, number % 100 FROM numbers(1000);
    CREATE HYPOTHETICAL PROJECTION p_all ON t_pruned (SELECT d, a, b ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_pruned WHERE d = '2030-05-05' AND b = 42 SETTINGS ${PIN};
    DROP TABLE t_pruned;
" | grep -E '^\s+(status|reason|verdict):' | awk '{$1=$1; print}'

echo "--- the preferred-projection setting only narrows existing projections, so it is ignored ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_est WHERE b = 42 SETTINGS ${PIN}, preferred_optimize_projection_name = 'p_other';
" 2>&1 | grep -E '^\s+(status|verdict):|Code:' | awk '{$1=$1; print}'

echo "--- a descending projection key does not parse ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL PROJECTION p_desc ON t_est (SELECT a, b, v ORDER BY b DESC);" 2>&1 | grep -m1 -oE 'SYNTAX_ERROR'

# the scan reads whole parts while the query itself is pruned under the limit, so neither overflow
# mode may fail a statement the user could run
echo "--- a limit the query respects does not fail the estimate, in either overflow mode ---"
for mode in throw break; do
    $CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
        EXPLAIN WHATIF SELECT count() FROM t_est WHERE a < 100 AND b = 42 SETTINGS ${PIN}, max_rows_to_read = 300, read_overflow_mode = '${mode}';
    " 2>&1 | grep -E '^\s+(status|source|empirical_status|empirical_reason):|Code:' | awk '{$1=$1; print}'
done

# a real projection lets the base analysis exceed the limit without throwing, the scan still degrades
echo "--- the base read exceeding the limit does not produce a verdict either ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_lim;
    CREATE TABLE t_lim (a UInt64, b UInt64, v UInt64, PROJECTION p_real (SELECT a, b, v ORDER BY b))
        ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_lim SELECT number, number % 100, number FROM numbers(1000);
    CREATE HYPOTHETICAL PROJECTION p_h ON t_lim (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT b, v FROM t_lim WHERE a < 500 AND b >= 15 SETTINGS ${PIN}, max_rows_to_read = 300;
" 2>&1 | grep -E '^\s+(marks|rows|verdict|empirical_status|empirical_reason):' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_lim;"

# the scan reads the projection columns, so SELECT is checked at estimate time
echo "--- estimating needs SELECT on the projection columns ---"
user="u_estimate_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${user}; CREATE USER ${user} NOT IDENTIFIED;
    GRANT ALTER ADD PROJECTION ON ${CLICKHOUSE_DATABASE}.t_est TO ${user};
    GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.t_est TO ${user};
"
$CLICKHOUSE_CLIENT --user "${user}" -q "
    CREATE HYPOTHETICAL PROJECTION p_priv ON ${CLICKHOUSE_DATABASE}.t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_est WHERE b = 42 SETTINGS ${PIN};
" 2>&1 | grep -m1 -oE 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "GRANT SELECT(v) ON ${CLICKHOUSE_DATABASE}.t_est TO ${user};"
$CLICKHOUSE_CLIENT --user "${user}" -q "
    CREATE HYPOTHETICAL PROJECTION p_priv ON ${CLICKHOUSE_DATABASE}.t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_est WHERE b = 42 SETTINGS ${PIN};
" 2>&1 | grep -E '^\s+status:' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est; DROP TABLE IF EXISTS t_real; DROP TABLE IF EXISTS t_est_g; DROP TABLE IF EXISTS t_real_g;"
