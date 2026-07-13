-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- Join-order cardinality estimation must respect partition/PK pruning (issue #110281).

DROP TABLE IF EXISTS fact_04516;
DROP TABLE IF EXISTS dim_04516;

-- refresh_statistics_interval = 0: no background statistics refresh, so the
-- table-wide estimator cache stays cold and the test is deterministic.
CREATE TABLE fact_04516 (p UInt8, id UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS refresh_statistics_interval = 0;

CREATE TABLE dim_04516 (id UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS refresh_statistics_interval = 0;

SET materialize_statistics_on_insert = 1;

-- partition p=1: 100,000 rows; partition p=2: 1,000 rows (one part each)
INSERT INTO fact_04516 SELECT 1, number FROM numbers(100000);
INSERT INTO fact_04516 SELECT 2, number % 10 FROM numbers(1000);
INSERT INTO dim_04516 SELECT number FROM numbers(10000);

-- The estimate for the fact side must reflect the pruned partition (1,000 rows),
-- not all parts (101,000 rows / NDV(p) = 50,500).
SELECT trimLeft(explain)
FROM (
    EXPLAIN
    SELECT count()
    FROM fact_04516 AS f
    INNER JOIN dim_04516 AS d ON f.id = d.id
    WHERE f.p = 2
    SETTINGS use_statistics = 1, use_statistics_cache = 0, collect_hash_table_stats_during_joins = 0
)
WHERE explain LIKE '%⋈%';

-- Same with the statistics cache enabled: a pruned query must not be served
-- by the table-wide cached estimator.
SELECT trimLeft(explain)
FROM (
    EXPLAIN
    SELECT count()
    FROM fact_04516 AS f
    INNER JOIN dim_04516 AS d ON f.id = d.id
    WHERE f.p = 2
    SETTINGS use_statistics = 1, use_statistics_cache = 1, collect_hash_table_stats_during_joins = 0
)
WHERE explain LIKE '%⋈%';

-- Control: without pruning the estimate covers the whole table.
SELECT trimLeft(explain)
FROM (
    EXPLAIN
    SELECT count()
    FROM fact_04516 AS f
    INNER JOIN dim_04516 AS d ON f.id = d.id
    SETTINGS use_statistics = 1, use_statistics_cache = 0, collect_hash_table_stats_during_joins = 0
)
WHERE explain LIKE '%⋈%';

-- NDV oracle: f[100] = 1000 rows x 1/NDV(id)=10 in the surviving part is derivable
-- only from pruned column statistics; the index-based fallback (use_statistics = 0)
-- yields f[1000] instead, so this probe fails if the statistics path silently degrades.
SELECT trimLeft(explain)
FROM (
    EXPLAIN
    SELECT count()
    FROM fact_04516 AS f
    INNER JOIN dim_04516 AS d ON f.id = d.id
    WHERE f.p = 2 AND f.id = 5
    SETTINGS use_statistics = 1, use_statistics_cache = 0, collect_hash_table_stats_during_joins = 0
)
WHERE explain LIKE '%⋈%';

-- PK-only pruning (no partitioning): two parts with disjoint id ranges in one
-- partition; WHERE id < 1000 prunes the second part entirely via the primary key.
-- Statistics must be composed over the surviving part: f[1000]
-- (all-parts estimate would be 2000 x (999 / 100999) ~ f[19]).
DROP TABLE IF EXISTS pk_04516;
CREATE TABLE pk_04516 (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS refresh_statistics_interval = 0;

SYSTEM STOP MERGES pk_04516;
INSERT INTO pk_04516 SELECT number, number FROM numbers(1000);
INSERT INTO pk_04516 SELECT number + 100000, number FROM numbers(1000);

SELECT trimLeft(explain)
FROM (
    EXPLAIN
    SELECT count()
    FROM pk_04516 AS f
    INNER JOIN dim_04516 AS d ON f.id = d.id
    WHERE f.id < 1000
    SETTINGS use_statistics = 1, use_statistics_cache = 0, collect_hash_table_stats_during_joins = 0
)
WHERE explain LIKE '%⋈%';

DROP TABLE pk_04516;
DROP TABLE fact_04516;
DROP TABLE dim_04516;
