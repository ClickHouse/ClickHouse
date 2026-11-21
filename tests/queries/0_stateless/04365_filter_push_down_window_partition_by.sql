-- Tags: no-parallel-replicas
-- ^ EXPLAIN indexes=1 asserts the pushed key predicate becomes the local primary key
-- condition; under parallel replicas the table is read on remote replicas, so the
-- coordinator plan carries no such condition and the assertions do not hold.
-- Filter push down below WindowStep on PARTITION BY columns (issue #110109).
-- A predicate referencing only the window PARTITION BY columns is safe to apply before
-- the window, so it must reach storage as a primary key condition and enable pruning.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;

DROP TABLE IF EXISTS t_04365;
CREATE TABLE t_04365 (key String, ts DateTime, val UInt64)
ENGINE = MergeTree ORDER BY (key, ts)
AS SELECT toString(number % 100) AS key, toDateTime(number) AS ts, number AS val
FROM numbers(100000);
OPTIMIZE TABLE t_04365 FINAL;

-- Helper output: `pushed` = 1 when the partition-key predicate reached ReadFromMergeTree
-- as a primary key Condition (so it was pushed below the window). Matching the
-- ReadFromMergeTree "Condition:" line is robust to randomized index_granularity /
-- plan-shape settings (no arithmetic on the granule counts, which can differ or be
-- absent under some randomized settings).

-- QUALIFY form: partition-key conjunct pushed below Window -> PK condition on storage.
SELECT count() > 0 AS pushed FROM (
    EXPLAIN indexes = 1
    SELECT key, ts, row_number() OVER (PARTITION BY key ORDER BY ts) AS rn
    FROM t_04365 QUALIFY rn = 1 AND key = '5'
) WHERE explain ILIKE '%Condition:%key%';

-- Outer WHERE around a windowed subquery: same push down.
SELECT count() > 0 AS pushed FROM (
    EXPLAIN indexes = 1
    SELECT * FROM (
        SELECT key, ts, row_number() OVER (PARTITION BY key ORDER BY ts) AS rn FROM t_04365
    ) WHERE key = '5'
) WHERE explain ILIKE '%Condition:%key%';

-- Multiple windows sharing the partition key: the common conjunct still reaches storage.
SELECT count() > 0 AS pushed FROM (
    EXPLAIN indexes = 1
    SELECT key, ts,
        row_number() OVER (PARTITION BY key ORDER BY ts) AS rn,
        sum(val)     OVER (PARTITION BY key ORDER BY ts) AS s
    FROM t_04365 QUALIFY rn = 1 AND key = '5'
) WHERE explain ILIKE '%Condition:%key%';

-- Mixed predicate: the partition-key conjunct pushes (PK condition on storage), the
-- window-result conjunct stays above.
SELECT count() > 0 AS pushed FROM (
    EXPLAIN indexes = 1
    SELECT key, ts, row_number() OVER (PARTITION BY key ORDER BY ts) AS rn
    FROM t_04365 QUALIFY rn = 1 AND key = '5' AND ts > toDateTime(10)
) WHERE explain ILIKE '%Condition:%key%';

-- Negative: a predicate on the window result only must NOT be pushed (no PK condition).
SELECT count() > 0 AS pushed FROM (
    EXPLAIN indexes = 1
    SELECT key, ts, row_number() OVER (PARTITION BY key ORDER BY ts) AS rn
    FROM t_04365 QUALIFY rn = 1
) WHERE explain ILIKE '%Condition:%key%';

-- No PARTITION BY: nothing to push, must not crash and must not push a PK condition.
SELECT count() > 0 AS pushed FROM (
    EXPLAIN indexes = 1
    SELECT key, ts, row_number() OVER (ORDER BY ts) AS rn
    FROM t_04365 QUALIFY rn = 1 AND key = '5'
) WHERE explain ILIKE '%Condition:%key%';

-- Negative: a non-deterministic conjunct on the partition key (`rand64(key) % 2 = 0`)
-- must NOT be pushed below the window. Pushing it would remove arbitrary rows inside a
-- surviving partition before the window runs and could change which row becomes
-- row_number() = 1. With prewhere pinned off, a pushed conjunct would appear as a
-- separate Filter step below the window (its `Filter column:` referencing rand64), so
-- the count of `Filter column:` lines mentioning rand64 must stay 1 (only the QUALIFY
-- filter kept above the window); a value of 2 means it was wrongly pushed down.
SELECT countIf(explain ILIKE '%Filter column:%rand64%') = 1 AS not_pushed FROM (
    EXPLAIN actions = 1
    SELECT key, ts, row_number() OVER (PARTITION BY key ORDER BY ts) AS rn
    FROM t_04365 QUALIFY rn = 1 AND rand64(key) % 2 = 0
    SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0
);

-- Correctness is preserved: the optimized query returns the same rows as forcing the
-- filter before the window explicitly.
SELECT count(), sum(val) FROM (
    SELECT key, ts, val, row_number() OVER (PARTITION BY key ORDER BY ts) AS rn
    FROM t_04365 QUALIFY rn = 1 AND key = '5'
);
SELECT count(), sum(val) FROM (
    SELECT key, ts, val, row_number() OVER (PARTITION BY key ORDER BY ts) AS rn
    FROM (SELECT * FROM t_04365 WHERE key = '5') QUALIFY rn = 1
);

DROP TABLE t_04365;

-- Old-analyzer regression: the pushdown must NOT disable the old-analyzer window-order
-- storage-order reuse (optimize_read_in_window_order). With the partition-key filter
-- pushed below the window, ReadFromMergeTree must still read in storage order
-- (Read type: InOrder) rather than fall back to a full sort. `reuse_fires` = 1.
SET enable_analyzer = 0;
DROP TABLE IF EXISTS t_04365_old;
CREATE TABLE t_04365_old (key String, ts DateTime, val UInt64)
ENGINE = MergeTree ORDER BY (key, ts)
AS SELECT toString(number % 100) AS key, toDateTime(number) AS ts, number AS val
FROM numbers(100000);
OPTIMIZE TABLE t_04365_old FINAL;

SELECT count() > 0 AS reuse_fires FROM (
    EXPLAIN
    SELECT * FROM (
        SELECT key, ts, row_number() OVER (PARTITION BY key ORDER BY ts) AS rn FROM t_04365_old
    ) WHERE key = '5'
    SETTINGS query_plan_filter_push_down = 1, optimize_read_in_order = 0, optimize_read_in_window_order = 1
) WHERE explain ILIKE '%Read type: InOrder%';

DROP TABLE t_04365_old;
