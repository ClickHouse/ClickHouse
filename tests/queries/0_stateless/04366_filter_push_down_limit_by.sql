-- Tags: no-parallel-replicas
-- ^ EXPLAIN indexes=1 asserts the pushed key predicate becomes the local primary key
-- condition; under parallel replicas the table is read on remote replicas, so the
-- coordinator plan carries no such condition and the assertions do not hold.
-- Filter push down below LimitByStep on LIMIT BY key columns (issue #110112).
-- A predicate referencing only the LIMIT BY key columns removes whole groups, so it is
-- result-equivalent above or below the LIMIT BY and must reach storage as the driving
-- primary key condition. It is only pushed for `OFFSET 0` and `LIMIT >= 1`: only then is
-- every non-empty group guaranteed a surviving row, so a throwing key predicate is not
-- evaluated on a group the step would otherwise discard (see the exception-semantics test
-- below). OFFSET and negative LIMIT BY forms are intentionally NOT pushed.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;

DROP TABLE IF EXISTS t_04366;
CREATE TABLE t_04366 (key String, ts DateTime, val UInt64)
ENGINE = MergeTree ORDER BY (key, ts)
AS SELECT toString(number % 100) AS key, toDateTime(number) AS ts, number AS val
FROM numbers(100000);
OPTIMIZE TABLE t_04366 FINAL;

-- `pushed` = 1 when the pushed LIMIT BY-key predicate reaches storage and becomes the
-- driving primary key condition (`Condition: (key in ...)`). This anchors on the
-- PrimaryKey condition text, not on arbitrary `Granules:` lines (which can come from
-- unrelated index sections). = 0 when the filter stays above and the PK sees no condition.

-- LIMIT n BY (OFFSET 0, n >= 1): key-column conjunct pushed below LimitBy -> PK condition.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key = '5'
);

-- Same query with query_plan_filter_push_down_below_limit_by = 0: the conjunct stays above the
-- LIMIT BY, so the primary key gets no condition -> the plan shape used before 26.7.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key = '5' SETTINGS query_plan_filter_push_down_below_limit_by = 0
);

-- `filter_above_limitby` = 1 iff a Filter step exists AND its plan line precedes LimitBy's.
-- Absence of a primary key condition alone cannot tell "filter above LimitBy" from "filter
-- pushed below it but index analysis produced nothing", so the OFF path needs this placement
-- oracle too. Same shape as 04512_filter_push_down_limit_by_nondeterministic.
SELECT (has_filter AND fl < ll) AS filter_above_limitby
FROM (
    SELECT countIf(explain LIKE '%Filter (%') > 0 AS has_filter,
           minIf(ln, explain LIKE '%Filter (%') AS fl,
           minIf(ln, explain LIKE '%LimitBy%')  AS ll
    FROM (
        SELECT explain, rowNumberInAllBlocks() AS ln
        FROM (
            EXPLAIN
            SELECT * FROM (
                SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
            ) WHERE key = '5' SETTINGS query_plan_filter_push_down_below_limit_by = 0, enable_parallel_replicas = 0
        )
    )
);

-- The push-down pass also runs with `query_plan_filter_push_down = 0` once a JOIN runtime
-- filter has been added, so the setting has to be honoured on that path as well: at its default
-- the key conjunct still reaches below LimitBy (no Filter step above it -> 0), at 0 the conjunct
-- stays above LimitBy (-> 1). The two arms differ only in the new setting.
-- `query_plan_optimize_join_order_randomize = 0` (its default) is pinned in both arms because a
-- non-zero seed feeds the join order optimizer random cardinalities, which can reverse the two
-- join inputs; the reversed plan prints the right-hand branch first and `fl < ll` then compares
-- lines from different branches.
DROP TABLE IF EXISTS t_04366_rhs;
CREATE TABLE t_04366_rhs (key String) ENGINE = MergeTree ORDER BY key AS SELECT '5';

SELECT (has_filter AND fl < ll) AS filter_above_limitby
FROM (
    SELECT countIf(explain LIKE '%Filter (%') > 0 AS has_filter,
           minIf(ln, explain LIKE '%Filter (%') AS fl,
           minIf(ln, explain LIKE '%LimitBy%')  AS ll
    FROM (
        SELECT explain, rowNumberInAllBlocks() AS ln
        FROM (
            EXPLAIN
            SELECT l.key FROM (
                SELECT key, ts FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
            ) AS l INNER JOIN t_04366_rhs AS r ON l.key = r.key
            WHERE l.key = '5'
            SETTINGS query_plan_filter_push_down = 0, query_plan_filter_push_down_below_limit_by = 1,
                     enable_join_runtime_filters = 1, enable_parallel_replicas = 0,
                     query_plan_optimize_join_order_randomize = 0
        )
    )
);

SELECT (has_filter AND fl < ll) AS filter_above_limitby
FROM (
    SELECT countIf(explain LIKE '%Filter (%') > 0 AS has_filter,
           minIf(ln, explain LIKE '%Filter (%') AS fl,
           minIf(ln, explain LIKE '%LimitBy%')  AS ll
    FROM (
        SELECT explain, rowNumberInAllBlocks() AS ln
        FROM (
            EXPLAIN
            SELECT l.key FROM (
                SELECT key, ts FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
            ) AS l INNER JOIN t_04366_rhs AS r ON l.key = r.key
            WHERE l.key = '5'
            SETTINGS query_plan_filter_push_down = 0, query_plan_filter_push_down_below_limit_by = 0,
                     enable_join_runtime_filters = 1, enable_parallel_replicas = 0,
                     query_plan_optimize_join_order_randomize = 0
        )
    )
);

DROP TABLE t_04366_rhs;

-- LIMIT n OFFSET m BY (m > 0): NOT pushed. A group of size <= m is fully dropped, so a
-- pushed throwing key predicate could be evaluated on rows the query never reaches.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 2 OFFSET 1 BY key
    ) WHERE key = '5'
);

-- LIMIT -n BY (NegativeLimitByStep): NOT pushed (a small group can be fully trimmed).
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT -1 BY key
    ) WHERE key = '5'
);

-- LIMIT 0 BY (getGroupLength() == 0): NOT pushed. The step discards every row of every
-- group, so a pushed throwing key predicate would be evaluated on rows the original query
-- never reaches (see the LIMIT 0 BY exception-semantics regression below).
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 0 BY key
    ) WHERE key = '5'
);

-- Mixed predicate: the key conjunct pushes (PK condition on key), the non-key conjunct
-- stays above.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key = '5' AND val > 10
);

-- Negative: a predicate on a non-key column (ts is in the PK but NOT a LIMIT BY key) must
-- NOT be pushed below LimitBy, so the primary key gets no condition on it.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE ts > toDateTime(99990)
);

-- Correctness: pushing the key filter below LIMIT BY must not change the result.
SELECT count(), sum(val) FROM (
    SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
) WHERE key = '5';

-- Same result when the filter is forced before the LIMIT BY.
SELECT count(), sum(val) FROM (
    SELECT key, ts, val FROM t_04366 WHERE key = '5' ORDER BY key, ts LIMIT 1 BY key
);

-- Same result with the push-down declined, so the OFF path is not covered by EXPLAIN alone.
SELECT count(), sum(val) FROM (
    SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
) WHERE key = '5' SETTINGS query_plan_filter_push_down_below_limit_by = 0;

DROP TABLE t_04366;

-- Exception-semantics regression: a singleton group '0' dropped by OFFSET 1 must NOT be
-- evaluated by a throwing key predicate. The un-pushed query returns empty (the group is
-- discarded before WHERE); pushing intDiv(1, toInt32(key)) below LIMIT BY would raise
-- Division by zero on the input row. Because OFFSET != 0 is not pushed, this stays empty.
DROP TABLE IF EXISTS t_04366_single;
CREATE TABLE t_04366_single (key String, val UInt64) ENGINE = MergeTree ORDER BY key
AS SELECT '0' AS key, number AS val FROM numbers(1);
SELECT * FROM (
    SELECT key, val FROM t_04366_single ORDER BY key LIMIT 1 OFFSET 1 BY key
) WHERE intDiv(1, toInt32(key)) > 0;

-- Same exception-semantics regression for `LIMIT 0 BY` (getGroupLength() == 0): the group
-- is fully discarded, so the un-pushed query is empty. Pushing intDiv(1, toInt32(key))
-- below LIMIT BY would raise Division by zero on the input row. Because LIMIT 0 BY is not
-- pushed, this stays empty rather than raising.
SELECT * FROM (
    SELECT key, val FROM t_04366_single ORDER BY key LIMIT 0 BY key
) WHERE intDiv(1, toInt32(key)) > 0;
DROP TABLE t_04366_single;

-- Semantic negative for non-key predicates: a throwing predicate on a NON-key column must
-- stay ABOVE LimitBy. Group 'a' has two rows ordered by `ord`: the surviving row (ord = 0)
-- has x = 1 (safe), the row LIMIT 1 BY drops (ord = 1) has x = 0. `intDiv(1, x)` therefore
-- only throws if evaluated on the dropped row, i.e. only if the non-key conjunct is pushed
-- below LimitBy. The un-pushed query returns the single surviving key without raising; a
-- text-only EXPLAIN check would miss such a regression, so assert the observable result.
DROP TABLE IF EXISTS t_04366_nonkey;
CREATE TABLE t_04366_nonkey (key String, ord UInt64, x Int64) ENGINE = MergeTree ORDER BY (key, ord)
AS SELECT 'a' AS key, number AS ord, if(number = 0, 1, 0) AS x FROM numbers(2);
SELECT key FROM (
    SELECT key, x FROM t_04366_nonkey ORDER BY key, ord LIMIT 1 BY key
) WHERE intDiv(1, x) > 0;
DROP TABLE t_04366_nonkey;

-- `compatibility` with a version before the one the setting was added in turns it off, so the
-- filter stays above the `LIMIT BY`; from that version on, and unset, it is on.
SET compatibility = '26.9';
SELECT getSetting('query_plan_filter_push_down_below_limit_by');
SET compatibility = '26.10';
SELECT getSetting('query_plan_filter_push_down_below_limit_by');
SET compatibility = '';
SELECT getSetting('query_plan_filter_push_down_below_limit_by');
