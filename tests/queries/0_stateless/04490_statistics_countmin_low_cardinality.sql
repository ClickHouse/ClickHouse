-- Tags: no-fasttest
-- no-fasttest: 'countmin' sketches need a 3rd party library

SET explain_query_plan_default = 'legacy';
SET allow_statistics = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'dpsize';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_merge_filter_into_join_condition = 1;
SET max_threads = 1;
SET min_insert_block_size_rows = 0;
SET min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS t_countmin_lc_src;
DROP TABLE IF EXISTS t_countmin_lc;

CREATE TABLE t_countmin_lc_src
(
    id UInt32,
    dense LowCardinality(UInt32),
    nullable LowCardinality(Nullable(String)),
    sparse LowCardinality(UInt32)
)
ENGINE = Memory;

CREATE TABLE t_countmin_lc
(
    id UInt32,
    dense LowCardinality(UInt32) STATISTICS(countmin),
    nullable LowCardinality(Nullable(String)) STATISTICS(countmin),
    sparse LowCardinality(UInt32) STATISTICS(countmin)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0;

INSERT INTO t_countmin_lc_src
SELECT
    number,
    toUInt32(multiIf(number % 6 < 3, 42, number % 6 < 5, 7, 199)),
    multiIf(number % 6 < 3, CAST(NULL, 'Nullable(String)'), number % 6 < 5, '', 'apple'),
    toUInt32(if(number < 200, number, multiIf(number % 6 < 3, 42, number % 6 < 5, 7, 199)))
FROM numbers(260)
SETTINGS max_block_size = 260;

-- The filtered block has 60 rows. `dense` and `nullable` have small dictionaries,
-- while `sparse` retains the source's 200-value dictionary but touches only three entries.
INSERT INTO t_countmin_lc
SELECT id, dense, nullable, sparse
FROM t_countmin_lc_src
WHERE id >= 200;

-- Join labels expose the CountMin equality estimates for each filtered relation.
SELECT countIf(explain LIKE '%d[30]%' AND explain LIKE '%n[10]%' AND explain LIKE '%s[30]%') = 1
FROM
(
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count()
    FROM t_countmin_lc AS d
    INNER JOIN t_countmin_lc AS n ON d.id = n.id
    INNER JOIN t_countmin_lc AS s ON n.id = s.id
    WHERE d.dense = 42 AND n.nullable = 'apple' AND s.sparse = 42
);

DROP TABLE t_countmin_lc;
DROP TABLE t_countmin_lc_src;
