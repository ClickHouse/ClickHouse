-- Tags: no-fasttest, no-replicated-database, no-random-merge-tree-settings
-- no-fasttest: column statistics require the full build
-- no-replicated-database: hypothetical indexes are session-scoped and not replicated
-- no-random-merge-tree-settings: the test requires deterministic index granularity

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET materialize_statistics_on_insert = 1;
SET enable_json_type = 1;

DROP TABLE IF EXISTS t_04695_stat;
DROP TABLE IF EXISTS t_04695_unrelated;
DROP TABLE IF EXISTS t_04695_insufficient;
DROP TABLE IF EXISTS t_04695_nullable;
DROP TABLE IF EXISTS t_04695_json;
DROP TABLE IF EXISTS t_04695_prewhere;

CREATE TABLE t_04695_stat
(
    a UInt64,
    b UInt64 STATISTICS(tdigest, uniq)
)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100, index_granularity_bytes = 0,
         min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_04695_stat SELECT number, number % 100 FROM numbers(10000);

CREATE TABLE t_04695_unrelated
(
    a UInt64,
    b UInt64,
    c UInt64 STATISTICS(tdigest, uniq)
)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100, index_granularity_bytes = 0,
         min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_04695_unrelated SELECT number, number % 100, number % 50 FROM numbers(10000);

CREATE TABLE t_04695_insufficient
(
    a UInt64,
    b UInt64 STATISTICS(uniq)
)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100, index_granularity_bytes = 0,
         min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_04695_insufficient SELECT number, number % 100 FROM numbers(10000);

CREATE TABLE t_04695_nullable
(
    a UInt64,
    n Nullable(UInt64) STATISTICS(basic)
)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100, index_granularity_bytes = 0,
         min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_04695_nullable SELECT number, if(number % 2, number, NULL) FROM numbers(10000);

CREATE TABLE t_04695_json
(
    a UInt64,
    x Nullable(JSON) STATISTICS(basic)
)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100, index_granularity_bytes = 0,
         min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_04695_json
SELECT number, if(number % 3 = 0, NULL, concat('{"null":', toString(number % 2), '}'))
FROM numbers(10000);

CREATE TABLE t_04695_prewhere
(
    s String STATISTICS(uniq),
    k UInt64 STATISTICS(tdigest)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '', default_compression_codec = 'NONE';
INSERT INTO t_04695_prewhere SELECT repeat('x', 100), number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_04695_unrelated ON t_04695_unrelated (b) TYPE minmax GRANULARITY 1;
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ')
FROM (EXPLAIN WHATIF empirical = 0 SELECT * FROM t_04695_unrelated WHERE b < 50)
WHERE match(explain, '^  (status|source|empirical_status):');

CREATE HYPOTHETICAL INDEX idx_04695_expression ON t_04695_stat (b % 10) TYPE minmax GRANULARITY 1;
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ')
FROM (EXPLAIN WHATIF empirical = 0 SELECT * FROM t_04695_stat WHERE b % 10 = 1)
WHERE match(explain, '^  (status|source|empirical_status):');

CREATE HYPOTHETICAL INDEX idx_04695_insufficient ON t_04695_insufficient (b) TYPE minmax GRANULARITY 1;
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ')
FROM (EXPLAIN WHATIF empirical = 0 SELECT * FROM t_04695_insufficient WHERE b < 50)
WHERE match(explain, '^  (status|source|empirical_status):');

CREATE HYPOTHETICAL INDEX idx_04695_nullable ON t_04695_nullable (n) TYPE set(100) GRANULARITY 1;
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ')
FROM
(
    EXPLAIN WHATIF empirical = 0
    SELECT * FROM t_04695_nullable WHERE isNull(n)
    SETTINGS optimize_functions_to_subcolumns = 1
)
WHERE match(explain, '^  (status|source|empirical_status):');

DROP HYPOTHETICAL INDEX idx_04695_nullable ON t_04695_nullable;
CREATE HYPOTHETICAL INDEX idx_04695_nullable ON t_04695_nullable (n.null) TYPE set(100) GRANULARITY 1;
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ')
FROM (EXPLAIN WHATIF empirical = 0 SELECT * FROM t_04695_nullable WHERE n.null = 1)
WHERE match(explain, '^  (status|source|empirical_status):');
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ')
FROM (EXPLAIN WHATIF empirical = 0 SELECT * FROM t_04695_nullable WHERE n.null = 0)
WHERE match(explain, '^  (status|source|empirical_status):');
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ')
FROM (EXPLAIN WHATIF empirical = 0 SELECT * FROM t_04695_nullable WHERE n.null != 1)
WHERE match(explain, '^  (status|source|empirical_status):');
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ')
FROM (EXPLAIN WHATIF empirical = 0 SELECT * FROM t_04695_nullable WHERE n.null != 0)
WHERE match(explain, '^  (status|source|empirical_status):');

CREATE HYPOTHETICAL INDEX idx_04695_json ON t_04695_json (x.null) TYPE set(100) GRANULARITY 1;
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ')
FROM (EXPLAIN WHATIF empirical = 0 SELECT * FROM t_04695_json WHERE x.null = 1)
WHERE match(explain, '^  (status|source|empirical_status):');

WITH
    (
        SELECT extractAll(explain, 'Prewhere filter column: ([^\\n]+)')[1]
        FROM
        (
            EXPLAIN actions = 1
            SELECT count() FROM t_04695_prewhere
            WHERE like(s, '%z%') AND k < 9000
            SETTINGS use_statistics = 1, optimize_move_to_prewhere = 1,
                     query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1,
                     move_all_conditions_to_prewhere = 1, move_primary_key_columns_to_end_of_prewhere = 1
        )
        WHERE explain LIKE '%Prewhere filter column%'
    ) AS with_statistics,
    (
        SELECT extractAll(explain, 'Prewhere filter column: ([^\\n]+)')[1]
        FROM
        (
            EXPLAIN actions = 1
            SELECT count() FROM t_04695_prewhere
            WHERE like(s, '%z%') AND k < 9000
            SETTINGS use_statistics = 0, optimize_move_to_prewhere = 1,
                     query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1,
                     move_all_conditions_to_prewhere = 1, move_primary_key_columns_to_end_of_prewhere = 1
        )
        WHERE explain LIKE '%Prewhere filter column%'
    ) AS without_statistics
SELECT notEmpty(with_statistics)
    AND with_statistics LIKE '%s%'
    AND with_statistics LIKE '%k%'
    AND with_statistics = without_statistics;

DROP TABLE t_04695_stat;
DROP TABLE t_04695_unrelated;
DROP TABLE t_04695_insufficient;
DROP TABLE t_04695_nullable;
DROP TABLE t_04695_json;
DROP TABLE t_04695_prewhere;
