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

-- A conjunct probing a set the plan cannot bound is NOT pushed below the LIMIT BY (issue #120341):
-- below the step it reaches index analysis, which materializes the whole set in sorted order, and it
-- is merged into the source filter, where `in` is not evaluated lazily and so runs on every source
-- row. A subquery set has no size until it is built, so it counts as unbounded.
DROP TABLE IF EXISTS t_04366_keys;
CREATE TABLE t_04366_keys (k String) ENGINE = MergeTree ORDER BY k
AS SELECT toString(number) AS k FROM numbers(50);

SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key IN (SELECT k FROM t_04366_keys)
);

-- Live-oracle control: `0` means no limit, the behaviour of 26.7 and 26.8, so the same query pushes again.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key IN (SELECT k FROM t_04366_keys)
    SETTINGS query_plan_max_set_size_for_filter_push_down_below_limit_by = 0
);

-- A literal list is a built set of known small size, so it still reaches the primary key.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key IN ('5', '7')
);

-- Only the unbounded conjunct is held back: the equality beside it still reaches the primary key.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key = '5' AND key IN (SELECT k FROM t_04366_keys)
);

-- Holding the conjunct above the LIMIT BY must not change the result.
SELECT count(), sum(val) FROM (
    SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
) WHERE key IN (SELECT k FROM t_04366_keys WHERE k = '5');

-- The bound is a row count, not a subquery test: a built literal set larger than it is held back
-- too. Four elements against a bound of three.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key IN ('5', '7', '11', '13')
    SETTINGS query_plan_max_set_size_for_filter_push_down_below_limit_by = 3
);

-- The same list at a bound equal to its size is pushed: the comparison is strictly greater-than.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key IN ('5', '7', '11', '13')
    SETTINGS query_plan_max_set_size_for_filter_push_down_below_limit_by = 4
);

-- `getTotalRowCount` deduplicates, while the sorted materialization filters the original list, so
-- the pre-deduplication length is bounded as well. Three distinct values in a six-entry list
-- against a bound of four: the deduplicated size fits and the list length does not.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key IN ('5', '5', '7', '7', '11', '11')
    SETTINGS query_plan_max_set_size_for_filter_push_down_below_limit_by = 4
);

-- The same list at a bound that covers its length passes both checks and is pushed.
SELECT countIf(match(explain, 'Condition: \(key in ')) > 0 AS pushed
FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, val FROM t_04366 ORDER BY key, ts LIMIT 1 BY key
    ) WHERE key IN ('5', '5', '7', '7', '11', '11')
    SETTINGS query_plan_max_set_size_for_filter_push_down_below_limit_by = 6
);

DROP TABLE t_04366_keys;

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
