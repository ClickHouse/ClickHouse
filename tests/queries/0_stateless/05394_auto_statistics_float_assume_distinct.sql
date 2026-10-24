-- With `auto_statistics_assume_floats_distinct`, implicit `uniq` / `uniq_v2` statistics are not built for
-- Float32 / Float64 columns. Where a cardinality is needed and no sketch exists, the optimizer assumes that
-- all non-NULL values are distinct, capped using `basic` (zeros count as one value, `min == max` is one value).
-- `system.parts_columns.estimates.cardinality` shows the assumed value.

SET allow_statistics = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET materialize_statistics_on_insert = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_floats;
DROP TABLE IF EXISTS t_floats_off;
DROP TABLE IF EXISTS t_floats_explicit;
DROP TABLE IF EXISTS t_floats_basic_only;
DROP TABLE IF EXISTS t_floats_uniq_only;
DROP TABLE IF EXISTS t_floats_prewhere;

-- `auto_statistics_types` and `materialize_statistics_on_*` are randomized by the test harness, so pin them.
CREATE TABLE t_floats
(
    f32 Float32,          -- distinct values, one of them zero
    f64 Float64,          -- 400 zeros, 600 distinct values
    f64_const Float64,    -- min == max
    nf64 Nullable(Float64), -- 250 NULLs
    bf16 BFloat16,        -- excluded: keeps its sketch
    lcf64 LowCardinality(Float64), -- excluded: keeps its sketch
    u64 UInt64,
    s String,
    f64_explicit Float64 STATISTICS(uniq_v2) -- explicit: keeps its sketch
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_floats_distinct = 1,
    materialize_statistics_on_merge = 1;

SELECT 'system.columns';
SELECT name, statistics FROM system.columns WHERE database = currentDatabase() AND table = 't_floats' ORDER BY position;

INSERT INTO t_floats
SELECT
    number,
    if(number < 400, 0, number),
    3.5,
    if(number % 4 = 0, NULL, number),
    number % 10,
    number % 3,
    number % 7,
    toString(number % 5),
    number % 10
FROM numbers(1000);

SELECT 'system.parts_columns';
SELECT column, statistics, estimates.cardinality, estimates.null_count, estimates.default_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats' AND active AND column IN ('f32', 'f64', 'f64_const', 'nf64', 'bf16', 'lcf64', 'u64', 's', 'f64_explicit')
ORDER BY column;

-- The setting is off: the implicit uniq is built, the cardinality comes from the sketch.
CREATE TABLE t_floats_off
(
    f64 Float64,
    nf64 Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_floats_distinct = 0,
    materialize_statistics_on_merge = 1;

INSERT INTO t_floats_off SELECT number % 10, if(number % 4 = 0, NULL, number % 10) FROM numbers(1000);

SELECT 'setting off';
SELECT name, statistics FROM system.columns WHERE database = currentDatabase() AND table = 't_floats_off' ORDER BY position;
SELECT column, statistics, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats_off' AND active AND column IN ('f64', 'nf64')
ORDER BY column;

-- Turn it on: the implicit uniq disappears from the metadata at once, the part keeps its sketch until it is merged.
ALTER TABLE t_floats_off MODIFY SETTING auto_statistics_assume_floats_distinct = 1;

SELECT 'setting on';
SELECT name, statistics FROM system.columns WHERE database = currentDatabase() AND table = 't_floats_off' ORDER BY position;
SELECT column, statistics, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats_off' AND active AND column IN ('f64', 'nf64')
ORDER BY column;

OPTIMIZE TABLE t_floats_off FINAL;

SELECT 'setting on, after merge';
SELECT column, statistics, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats_off' AND active AND column IN ('f64', 'nf64')
ORDER BY column;

-- Turn it off again: the implicit uniq is declared again, so nothing is assumed until the part has its sketch.
ALTER TABLE t_floats_off MODIFY SETTING auto_statistics_assume_floats_distinct = 0;

SELECT 'setting off again';
SELECT name, statistics FROM system.columns WHERE database = currentDatabase() AND table = 't_floats_off' ORDER BY position;
SELECT column, statistics, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats_off' AND active AND column IN ('f64', 'nf64')
ORDER BY column;

OPTIMIZE TABLE t_floats_off FINAL;

SELECT 'setting off again, after merge';
SELECT column, statistics, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats_off' AND active AND column IN ('f64', 'nf64')
ORDER BY column;

-- An explicit uniq that is not materialized yet: nothing is assumed.
CREATE TABLE t_floats_explicit
(
    x Float64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_floats_distinct = 1,
    materialize_statistics_on_merge = 1;

INSERT INTO t_floats_explicit SELECT number % 10 FROM numbers(1000);

SELECT 'explicit uniq';
SELECT column, statistics, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats_explicit' AND active AND column = 'x';

ALTER TABLE t_floats_explicit ADD STATISTICS x TYPE uniq_v2;

SELECT 'explicit uniq, added';
SELECT name, statistics FROM system.columns WHERE database = currentDatabase() AND table = 't_floats_explicit';
SELECT column, statistics, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats_explicit' AND active AND column = 'x';

ALTER TABLE t_floats_explicit MATERIALIZE STATISTICS x;

SELECT 'explicit uniq, materialized';
SELECT column, statistics, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats_explicit' AND active AND column = 'x';

ALTER TABLE t_floats_explicit DROP STATISTICS x;
ALTER TABLE t_floats_explicit MATERIALIZE STATISTICS ALL;

SELECT 'explicit uniq, dropped';
SELECT name, statistics FROM system.columns WHERE database = currentDatabase() AND table = 't_floats_explicit';
SELECT column, statistics, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats_explicit' AND active AND column = 'x';

-- No uniq configured at all: the values are assumed to be distinct as well.
CREATE TABLE t_floats_basic_only
(
    f64 Float64,
    u64 UInt64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic',
    auto_statistics_assume_floats_distinct = 1,
    materialize_statistics_on_merge = 1;

INSERT INTO t_floats_basic_only SELECT number, number FROM numbers(1000);

SELECT 'basic only';
SELECT column, statistics, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats_basic_only' AND active AND column IN ('f64', 'u64')
ORDER BY column;

-- Only uniq configured: Float columns get no implicit statistics at all.
CREATE TABLE t_floats_uniq_only
(
    f64 Float64,
    u64 UInt64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'uniq_v2',
    auto_statistics_assume_floats_distinct = 1,
    materialize_statistics_on_merge = 1;

INSERT INTO t_floats_uniq_only SELECT number % 10, number % 10 FROM numbers(1000);

SELECT 'uniq only';
SELECT name, statistics FROM system.columns WHERE database = currentDatabase() AND table = 't_floats_uniq_only' ORDER BY position;
SELECT column, statistics, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_floats_uniq_only' AND active AND column IN ('f64', 'u64')
ORDER BY column;

-- The optimizer uses the assumed cardinality, e.g. to order PREWHERE conditions.
-- The `EXPLAIN` lines are trimmed: their indentation depends on the plan, e.g. with parallel replicas.
SET explain_query_plan_default = 'legacy';
SET use_statistics = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1;

CREATE TABLE t_floats_prewhere
(
    f Float64,
    i Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    auto_statistics_types = 'basic, uniq_v2',
    auto_statistics_assume_floats_distinct = 1,
    materialize_statistics_on_merge = 1,
    -- Compact parts: column sizes are not measured, so the order depends on the estimated selectivity only.
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000;

INSERT INTO t_floats_prewhere SELECT number + 0.5, number % 1000 FROM numbers(10000);

-- The first query caches the statistics of the part. The policy is applied on top of them for every query,
-- so the setting change below takes effect although the cached statistics stay the same.
-- `f = 100.5` is assumed to match one row, `i = 10` matches about ten: `f` is evaluated first.
SELECT 'prewhere, assumed';
SELECT trimLeft(replaceRegexpAll(explain, '__table1\.', ''))
FROM (EXPLAIN actions = 1 SELECT count() FROM t_floats_prewhere WHERE i = 10 AND f = 100.5)
WHERE explain LIKE '%Prewhere filter column%';

ALTER TABLE t_floats_prewhere MODIFY SETTING auto_statistics_assume_floats_distinct = 0;

-- No sketch for `f` and nothing assumed: the default selectivity, so `i` is evaluated first.
SELECT 'prewhere, not assumed';
SELECT trimLeft(replaceRegexpAll(explain, '__table1\.', ''))
FROM (EXPLAIN actions = 1 SELECT count() FROM t_floats_prewhere WHERE i = 10 AND f = 100.5)
WHERE explain LIKE '%Prewhere filter column%';

DROP TABLE t_floats;
DROP TABLE t_floats_off;
DROP TABLE t_floats_explicit;
DROP TABLE t_floats_basic_only;
DROP TABLE t_floats_uniq_only;
DROP TABLE t_floats_prewhere;
