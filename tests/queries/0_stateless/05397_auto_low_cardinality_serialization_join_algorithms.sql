-- Tags: no-old-analyzer

-- Join inputs read from a table that has both `Default` and `LowCardinality` (automatic
-- `LowCardinality` serialization) parts: the chunks of the encoded column arrive with different
-- in-memory representations, and every join algorithm must accept both on either side.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS t_auto_lc_join;
CREATE TABLE t_auto_lc_join
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 100,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_auto_lc_join;

-- High cardinality -> `Default`.
INSERT INTO t_auto_lc_join SELECT number, 'a_' || toString(number) FROM numbers(300);
-- Low cardinality -> `LowCardinality`.
INSERT INTO t_auto_lc_join SELECT number, 'b_' || toString(number % 10) FROM numbers(300, 300);

SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_join' AND active AND column = 'lc'
ORDER BY serialization_kind;

-- An inequality in an outer join is evaluated by the block nested loop join.
SELECT 'block nested loop';
SELECT count(), max(l.lc), max(r.lc)
FROM t_auto_lc_join AS l LEFT JOIN t_auto_lc_join AS r ON l.lc < r.lc AND r.id % 7 = 0
SETTINGS max_threads = 1, max_block_size = 100;

SELECT count(), max(l.lc), max(r.lc)
FROM t_auto_lc_join AS l LEFT JOIN t_auto_lc_join AS r ON NOT (l.lc != r.lc)
SETTINGS max_threads = 1, max_block_size = 100;

SELECT count(), max(l.lc), max(r.lc)
FROM merge(currentDatabase(), '^t_auto_lc_join$') AS l
GLOBAL LEFT JOIN remote('127.0.0.1', currentDatabase(), 't_auto_lc_join') AS r ON NOT (l.lc != r.lc);

-- Equality joins, with the encoded column as the key and as a payload of either side.
SELECT 'full_sorting_merge';
SELECT count(), max(l.lc), max(r.lc)
FROM t_auto_lc_join AS l INNER JOIN t_auto_lc_join AS r USING (id)
SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 1, max_block_size = 100;
SELECT count(), max(l.id), max(r.id)
FROM t_auto_lc_join AS l INNER JOIN t_auto_lc_join AS r ON l.lc = r.lc
SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 1, max_block_size = 100;

SELECT 'partial_merge';
SELECT count(), max(l.lc), max(r.lc)
FROM t_auto_lc_join AS l INNER JOIN t_auto_lc_join AS r USING (id)
SETTINGS join_algorithm = 'partial_merge', max_threads = 1, max_block_size = 100;
SELECT count(), max(l.id), max(r.id)
FROM t_auto_lc_join AS l INNER JOIN t_auto_lc_join AS r ON l.lc = r.lc
SETTINGS join_algorithm = 'partial_merge', max_threads = 1, max_block_size = 100;

SELECT 'hash';
SELECT count(), max(l.lc), max(r.lc)
FROM t_auto_lc_join AS l INNER JOIN t_auto_lc_join AS r USING (id)
SETTINGS join_algorithm = 'hash', max_threads = 1, max_block_size = 100;

SELECT 'grace_hash';
SELECT count(), max(l.lc), max(r.lc)
FROM t_auto_lc_join AS l INNER JOIN t_auto_lc_join AS r USING (id)
SETTINGS join_algorithm = 'grace_hash', max_threads = 1, max_block_size = 100;

DROP TABLE t_auto_lc_join;
