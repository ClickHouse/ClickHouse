-- The per-column default counters must be refused while a part still holds the pre-`ALTER` type
-- and used again once the part has been rewritten with the new one.

DROP TABLE IF EXISTS t_sparse_alter;
SET optimize_trivial_count_query = 1, optimize_trivial_count_with_sparsity_filter = 1;

DROP TABLE IF EXISTS t_sparse_alter;

CREATE TABLE t_sparse_alter (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, remove_empty_parts = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1,
         ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         nullable_serialization_version = 'allow_sparse';

INSERT INTO t_sparse_alter SELECT number, if(number % 10 = 0, 0, number) FROM numbers(1000);

SELECT 'before alter', count() FROM t_sparse_alter WHERE v = 0;
SELECT 'before alter served', countIf(explain LIKE '%Optimized trivial count with sparsity filter%')
    FROM (EXPLAIN SELECT count() FROM t_sparse_alter WHERE v = 0);

ALTER TABLE t_sparse_alter MODIFY COLUMN v Nullable(UInt64) SETTINGS mutations_sync = 2;

SELECT 'after alter', count() FROM t_sparse_alter WHERE v IS NULL;
SELECT 'after alter served', countIf(explain LIKE '%Optimized trivial count with sparsity filter%')
    FROM (EXPLAIN SELECT count() FROM t_sparse_alter WHERE v IS NULL);

OPTIMIZE TABLE t_sparse_alter FINAL;

SELECT 'after optimize', count() FROM t_sparse_alter WHERE v IS NULL;
SELECT 'after optimize not null', count() FROM t_sparse_alter WHERE v IS NOT NULL;
SELECT 'after optimize served', countIf(explain LIKE '%Optimized trivial count with sparsity filter%')
    FROM (EXPLAIN SELECT count() FROM t_sparse_alter WHERE v IS NULL);

-- A part that lost all of its rows is skipped; the counters of the remaining parts still answer.
INSERT INTO t_sparse_alter SELECT 10000 + number, NULL FROM numbers(10);
ALTER TABLE t_sparse_alter DELETE WHERE k >= 10000 SETTINGS mutations_sync = 2;

SELECT 'empty part present', countIf(rows = 0) > 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 't_sparse_alter' AND active;
SELECT 'empty part total', count() FROM t_sparse_alter;
SELECT 'empty part', count() FROM t_sparse_alter WHERE v IS NULL;
SELECT 'empty part served', countIf(explain LIKE '%Optimized trivial count with sparsity filter%')
    FROM (EXPLAIN SELECT count() FROM t_sparse_alter WHERE v IS NULL);

DROP TABLE t_sparse_alter;
