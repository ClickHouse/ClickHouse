-- Tags: no-random-settings

-- Stress tests for TopN aggregation: skewed data, large composite keys, parallel streams.
-- Validates serialized-key correctness and threshold monotonicity under edge conditions.

-- ====== Large composite keys ======
-- Exercises the IColumn::serializeValueIntoArena path with multi-column keys
-- of varying types and widths. Uses 500 distinct groups with unique max(val).

DROP TABLE IF EXISTS t_topn_composite_key;
CREATE TABLE t_topn_composite_key
(
    k1 String,
    k2 UInt64,
    k3 FixedString(16),
    k4 String,
    val DateTime
)
ENGINE = MergeTree ORDER BY (k1, k2);

INSERT INTO t_topn_composite_key SELECT
    repeat('A', (grp % 37) + 1) || toString(grp),
    grp,
    leftPad(toString(grp), 16, '0'),
    'tag_' || toString(grp % 50),
    toDateTime('2024-01-01') + number
FROM (SELECT number, number % 500 AS grp FROM numbers(10000));

SELECT '-- composite key: diff count';
SELECT count() FROM (
    SELECT k1, k2, k3, k4, max(val) AS m
    FROM t_topn_composite_key
    GROUP BY k1, k2, k3, k4
    ORDER BY m DESC LIMIT 10
    SETTINGS optimize_topn_aggregation = 1
) AS opt
FULL OUTER JOIN (
    SELECT k1, k2, k3, k4, max(val) AS m
    FROM t_topn_composite_key
    GROUP BY k1, k2, k3, k4
    ORDER BY m DESC LIMIT 10
    SETTINGS optimize_topn_aggregation = 0
) AS ref USING (k1, k2, k3, k4, m)
WHERE opt.k1 = '' OR ref.k1 = '';

SELECT '-- composite key: top 20 diff count';
SELECT count() FROM (
    SELECT k1, k2, k3, k4, max(val) AS m
    FROM t_topn_composite_key
    GROUP BY k1, k2, k3, k4
    ORDER BY m DESC LIMIT 20
    SETTINGS optimize_topn_aggregation = 1
) AS opt
FULL OUTER JOIN (
    SELECT k1, k2, k3, k4, max(val) AS m
    FROM t_topn_composite_key
    GROUP BY k1, k2, k3, k4
    ORDER BY m DESC LIMIT 20
    SETTINGS optimize_topn_aggregation = 0
) AS ref USING (k1, k2, k3, k4, m)
WHERE opt.k1 = '' OR ref.k1 = '';

DROP TABLE t_topn_composite_key;

-- ====== Skewed data with heavy group overlap ======
-- Many groups share the same aggregate boundary value (ties at the K-th position).
-- Validates that parallel partial pruning + merge produces correct top K under ties.
-- Uses structural checks rather than exact row ordering (ties are non-deterministic).

DROP TABLE IF EXISTS t_topn_skew;
CREATE TABLE t_topn_skew (grp String, val UInt64)
ENGINE = MergeTree ORDER BY grp;

-- 200 groups. Groups 0..49 all have max = 9999 (tied at top).
-- Groups 50..199 have max = group_num, spread out below.
INSERT INTO t_topn_skew
SELECT
    'grp_' || leftPad(toString(number % 200), 3, '0'),
    if(number % 200 < 50, 9999, number % 200)
FROM numbers(20000);

SELECT '-- skewed ties: top 10 all have val=9999';
SELECT count(), min(m), max(m) FROM (
    SELECT grp, max(val) AS m
    FROM t_topn_skew
    GROUP BY grp
    ORDER BY m DESC
    LIMIT 10
    SETTINGS optimize_topn_aggregation = 1
);

SELECT '-- skewed ties: top 60 correct structure';
SELECT
    countIf(m = 9999) AS tied_count,
    countIf(m < 9999) AS non_tied_count,
    min(if(m < 9999, m, 99999)) AS min_non_tied
FROM (
    SELECT grp, max(val) AS m
    FROM t_topn_skew
    GROUP BY grp
    ORDER BY m DESC
    LIMIT 60
    SETTINGS optimize_topn_aggregation = 1
);

SELECT '-- skewed ties: top 60 reference structure';
SELECT
    countIf(m = 9999) AS tied_count,
    countIf(m < 9999) AS non_tied_count,
    min(if(m < 9999, m, 99999)) AS min_non_tied
FROM (
    SELECT grp, max(val) AS m
    FROM t_topn_skew
    GROUP BY grp
    ORDER BY m DESC
    LIMIT 60
    SETTINGS optimize_topn_aggregation = 0
);

DROP TABLE t_topn_skew;

-- ====== Parallel Mode 2 with threshold pruning on MergeTree ======
-- Larger dataset to exercise parallel partial workers with the __topKFilter prewhere.

DROP TABLE IF EXISTS t_topn_parallel_stress;
CREATE TABLE t_topn_parallel_stress (grp String, ts DateTime)
ENGINE = MergeTree ORDER BY grp;

INSERT INTO t_topn_parallel_stress SELECT
    'g_' || leftPad(toString(number % 5000), 4, '0'),
    toDateTime('2024-01-01') + number
FROM numbers(200000);

SELECT '-- parallel stress: max DESC diff count';
SELECT count() FROM (
    SELECT grp, max(ts) AS m
    FROM t_topn_parallel_stress
    GROUP BY grp ORDER BY m DESC LIMIT 20
    SETTINGS optimize_topn_aggregation = 1
) AS opt
FULL OUTER JOIN (
    SELECT grp, max(ts) AS m
    FROM t_topn_parallel_stress
    GROUP BY grp ORDER BY m DESC LIMIT 20
    SETTINGS optimize_topn_aggregation = 0
) AS ref USING (grp, m)
WHERE opt.grp = '' OR ref.grp = '';

SELECT '-- parallel stress: min ASC diff count';
SELECT count() FROM (
    SELECT grp, min(ts) AS m
    FROM t_topn_parallel_stress
    GROUP BY grp ORDER BY m ASC LIMIT 20
    SETTINGS optimize_topn_aggregation = 1
) AS opt
FULL OUTER JOIN (
    SELECT grp, min(ts) AS m
    FROM t_topn_parallel_stress
    GROUP BY grp ORDER BY m ASC LIMIT 20
    SETTINGS optimize_topn_aggregation = 0
) AS ref USING (grp, m)
WHERE opt.grp = '' OR ref.grp = '';

DROP TABLE t_topn_parallel_stress;
