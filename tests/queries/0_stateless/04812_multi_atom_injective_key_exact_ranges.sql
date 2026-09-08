-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: the test pins plan shape and rows-read limits
-- that depend on granule layout and projection settings.

-- An equality with a key column reachable both through the relaxed monotonic transform and
-- through the exact deterministic transform must produce an exact atom, not a relaxed one.
-- For `ORDER BY negate(x)` and `WHERE x = 5`, keeping the relaxed `negate(x) = -5` atom would
-- make the whole key condition relaxed and disable exact ranges, so `count()` would have to
-- read every selected granule instead of counting the fully-matched ones without reading.

DROP TABLE IF EXISTS t_injective_key;

CREATE TABLE t_injective_key (x Int64) ENGINE = MergeTree ORDER BY negate(x) SETTINGS index_granularity = 8;

INSERT INTO t_injective_key SELECT intDiv(number, 100) FROM numbers(1000);

-- The value 5 spans ~13 granules; with exact ranges only the two boundary granules are read.
-- Without them this reads all matching rows and trips the cap.
SELECT count() FROM t_injective_key WHERE x = 5 SETTINGS max_rows_to_read = 32, optimize_use_implicit_projections = 1;

-- The plan must count the fully-matched interior granules through the exact-count projection.
SELECT countIf(explain LIKE '%_exact_count_projection%') FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM t_injective_key WHERE x = 5
    SETTINGS optimize_use_implicit_projections = 1
);

DROP TABLE t_injective_key;
