#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The INSERT ... SELECT carrier below reaches the discard through InterpreterInsertQuery, which
# builds the inserted SELECT's pipeline itself, so explaining a standalone SELECT would not pin
# it. EXPLAIN over an INSERT emits a processor graph whose node numbering is not stable, so the
# processors are counted instead of compared.
explain_insert() {
    $CLICKHOUSE_CLIENT --enable_analyzer 1 --enable_materialized_cte 1 -q "
        DROP TABLE IF EXISTS t_04561_plan SYNC;
        CREATE TABLE t_04561_plan (id UInt64) ENGINE = Memory;
    "
    # With the setting disabled the analyzer warns that MATERIALIZED is ignored; keep it out of stderr.
    local quiet_logs=''
    [ "$1" = 0 ] && quiet_logs="SET send_logs_level = 'error';"
    $CLICKHOUSE_CLIENT --enable_analyzer 1 --enable_materialized_cte "$1" -q "
        $quiet_logs
        EXPLAIN PIPELINE INSERT INTO t_04561_plan
            WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
            SELECT id FROM a WHERE id IN (SELECT id FROM a) GROUP BY id WITH TOTALS;
    " | grep -cE 'MaterializingCTETransform|DroppingTransform'
    $CLICKHOUSE_CLIENT -q "DROP TABLE t_04561_plan SYNC;"
}

# 2 with materialization (the CTE writer and the totals discard), 1 without it.
explain_insert 1
explain_insert 0

$CLICKHOUSE_CLIENT --multiquery -q "$(cat <<'SQL'
-- Tests that a materialized CTE read on a branch feeding a dropped WITH TOTALS port is still
-- gated by the DelayedPortsProcessor. Previously the totals port was dropped via a childless
-- NullSink, which ExecutingGraph::initializeExecution seeds, pulling the CTE reader before its
-- gate opened: "Reading from materialized CTE '...' before its materialization completed -
-- DelayedPortsProcessor gate is missing in the query plan" (LOGICAL_ERROR, STID 2467-2c2d).

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

-- Each carrier below asserts a row count, which is equal whether the CTE is materialized or
-- inlined, so each is preceded by an assertion that its own shape is planned with
-- materialization. Otherwise a carrier would keep passing if planning stopped materializing on
-- just that path, which is the state whose gate this test covers.

-- Aggregation over a WITH TOTALS subquery that reads a materialized CTE both directly and inside an IN-subquery.
SELECT countIf(explain LIKE '%MaterializingCTETransform%') > 0 FROM viewExplain('EXPLAIN PIPELINE', '', (
    SELECT count() FROM (
        WITH a AS MATERIALIZED (SELECT 1 AS id)
        SELECT id FROM a WHERE id IN (SELECT id FROM a) GROUP BY id WITH TOTALS
    )
));
SELECT count() FROM (
    WITH a AS MATERIALIZED (SELECT 1 AS id)
    SELECT id FROM a WHERE id IN (SELECT id FROM a) GROUP BY id WITH TOTALS
);

-- Same, verifying the result matches the non-materialized CTE equivalent.
SELECT countIf(explain LIKE '%MaterializingCTETransform%') > 0 FROM viewExplain('EXPLAIN PIPELINE', '', (
    SELECT count() FROM (
        WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(100))
        SELECT id FROM a WHERE id IN (SELECT id FROM a) GROUP BY id WITH TOTALS
    )
));
SELECT count() FROM (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(100))
    SELECT id FROM a WHERE id IN (SELECT id FROM a) GROUP BY id WITH TOTALS
);
SELECT count() FROM (
    WITH a AS (SELECT number AS id FROM numbers(100))
    SELECT id FROM a WHERE id IN (SELECT id FROM a) GROUP BY id WITH TOTALS
);

-- INSERT ... SELECT drops totals via a different code path (InterpreterInsertQuery), same class of
-- bug. Its plan assertion is in the shell part of this test: EXPLAIN over an INSERT emits a
-- processor graph with unstable node numbering, so it is grepped rather than compared.
DROP TABLE IF EXISTS t_04561 SYNC;
CREATE TABLE t_04561 (id UInt64) ENGINE = Memory;
INSERT INTO t_04561
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(10))
    SELECT id FROM a WHERE id IN (SELECT id FROM a) GROUP BY id WITH TOTALS;
SELECT count() FROM t_04561;
DROP TABLE t_04561 SYNC;

-- Original AST-fuzzer reproducer shape (issue #110176): materialized CTE reused across a doubly
-- nested IN-subquery with WITH TOTALS at two levels.
DROP TABLE IF EXISTS t_04561_src SYNC;
CREATE TABLE t_04561_src (id UInt64) ENGINE = Memory;
INSERT INTO t_04561_src SELECT number FROM numbers(50);
SELECT countIf(explain LIKE '%MaterializingCTETransform%') > 0 FROM viewExplain('EXPLAIN PIPELINE', '', (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(100))
    SELECT id FROM a
    WHERE id IN (
        SELECT id FROM t_04561_src
        WHERE id IN (SELECT id FROM a GROUP BY id)
        GROUP BY id WITH TOTALS)
    GROUP BY id WITH TOTALS
));
SELECT count() IGNORE NULLS FROM (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(100))
    SELECT id FROM a
    WHERE id IN (
        SELECT id FROM t_04561_src
        WHERE id IN (SELECT id FROM a GROUP BY id)
        GROUP BY id WITH TOTALS)
    GROUP BY id WITH TOTALS);
DROP TABLE t_04561_src SYNC;

-- A dropped totals port must not be read: each group here has one row while the totals row has
-- three, so a `HAVING` evaluated on the totals row would raise FUNCTION_THROW_IF_VALUE_IS_NON_ZERO.
SELECT count() FROM (SELECT number AS k FROM numbers(3) GROUP BY k WITH TOTALS HAVING throwIf(count() = 3) = 0);

DROP TABLE IF EXISTS t_04561_dst SYNC;
CREATE TABLE t_04561_dst (k UInt64) ENGINE = Memory;
INSERT INTO t_04561_dst SELECT number AS k FROM numbers(3) GROUP BY k WITH TOTALS HAVING throwIf(count() = 3) = 0;
SELECT count() FROM t_04561_dst;
DROP TABLE t_04561_dst SYNC;

-- Same through a materialized view's dependent insert.
DROP TABLE IF EXISTS t_04561_mv_src SYNC;
DROP TABLE IF EXISTS t_04561_mv SYNC;
DROP TABLE IF EXISTS t_04561_mv_dst SYNC;
CREATE TABLE t_04561_mv_src (k UInt64) ENGINE = Memory;
CREATE TABLE t_04561_mv_dst (k UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW t_04561_mv TO t_04561_mv_dst AS
    SELECT k FROM t_04561_mv_src GROUP BY k WITH TOTALS HAVING throwIf(count() = 3) = 0;
INSERT INTO t_04561_mv_src SELECT number FROM numbers(3);
SELECT count() FROM t_04561_mv_dst;
DROP TABLE t_04561_mv SYNC;
DROP TABLE t_04561_mv_dst SYNC;
DROP TABLE t_04561_mv_src SYNC;
SQL
)"
