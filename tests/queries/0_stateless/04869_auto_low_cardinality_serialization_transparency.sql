-- Regression tests for the transparency of automatic LowCardinality serialization:
-- the setting must work independently of sparse serialization, a table with mixed
-- `Default` and `LowCardinality` parts must be queryable, and an explicit subcolumn
-- read of an encoded column must fail with a clear message instead of an internal error.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

-- 1) `max_uniq_number_for_low_cardinality` must not depend on sparse serialization being enabled.
DROP TABLE IF EXISTS t_auto_lc_no_sparse;
CREATE TABLE t_auto_lc_no_sparse
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 1,  -- sparse serialization fully disabled
    max_uniq_number_for_low_cardinality = 1000,
    min_bytes_for_wide_part = 0;

INSERT INTO t_auto_lc_no_sparse SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);

SELECT 'sparse disabled: kind, correctness';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_no_sparse' AND active AND column = 'lc';
SELECT count(), uniqExact(lc) FROM t_auto_lc_no_sparse;

-- The `optimize_functions_to_subcolumns` rewrite of `length(lc)` to `lc.size` must still be skipped,
-- which requires the table-level serialization hints to know about the kind.
SELECT 'sparse disabled: subcolumn rewrite is skipped';
SET optimize_functions_to_subcolumns = 1;
SELECT sum(length(lc)) FROM t_auto_lc_no_sparse;
SELECT count() FROM t_auto_lc_no_sparse WHERE notEmpty(lc);

-- Existing encoded parts must keep their table-level hint after the setting for new writes is disabled.
ALTER TABLE t_auto_lc_no_sparse MODIFY SETTING max_uniq_number_for_low_cardinality = 0;
DETACH TABLE t_auto_lc_no_sparse;
ATTACH TABLE t_auto_lc_no_sparse;
SELECT 'sparse disabled after reload: subcolumn rewrite is skipped';
SELECT sum(length(lc)) FROM t_auto_lc_no_sparse;

-- 2) An explicit subcolumn read of an encoded column is rejected with a clear message.
SELECT 'explicit subcolumn read is rejected';
SELECT sum(lc.size) FROM t_auto_lc_no_sparse; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_auto_lc_no_sparse;

-- 3) Mixed parts: one active part stored as `Default`, another as `LowCardinality`.
DROP TABLE IF EXISTS t_auto_lc_mixed;
CREATE TABLE t_auto_lc_mixed
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 100,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_auto_lc_mixed;

-- High cardinality -> `Default`.
INSERT INTO t_auto_lc_mixed SELECT number, 'a_' || toString(number) FROM numbers(2000);
-- Low cardinality -> `LowCardinality`.
INSERT INTO t_auto_lc_mixed SELECT number, 'b_' || toString(number % 10) FROM numbers(2000, 2000);

SELECT 'mixed parts: kinds';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_mixed' AND active AND column = 'lc'
ORDER BY serialization_kind;

-- Every query below combines chunks that come from both parts. `max_threads = 1` puts the chunks of
-- both parts through the same transform, which is what makes the representations meet.
SELECT 'mixed parts: order by encoded column';
SELECT lc FROM t_auto_lc_mixed ORDER BY lc, id LIMIT 3;
SELECT lc FROM t_auto_lc_mixed ORDER BY lc DESC, id LIMIT 3;

-- `PartialSortingTransform` keeps the threshold row of a previous chunk and compares the next chunk
-- against it. The threshold optimization needs a limit of at least 1500 rows, or the top-k threshold
-- tracker (which for a `String` sort column also needs `use_top_k_dynamic_filtering_for_variable_length_types`).
SELECT 'mixed parts: partial sorting threshold';
SELECT count(), min(lc), max(lc) FROM (SELECT lc FROM t_auto_lc_mixed ORDER BY lc LIMIT 1600)
SETTINGS max_threads = 1, max_block_size = 2000;
SELECT count(), min(lc), max(lc) FROM (SELECT lc FROM t_auto_lc_mixed ORDER BY lc DESC LIMIT 1600)
SETTINGS max_threads = 1, max_block_size = 2000;
SELECT lc FROM t_auto_lc_mixed ORDER BY lc LIMIT 3
SETTINGS max_threads = 1, max_block_size = 100, use_top_k_dynamic_filtering = 1,
    use_top_k_dynamic_filtering_for_variable_length_types = 1, query_plan_max_limit_for_top_k_optimization = 1000;

-- `FinishSortingTransform` compares the tail of the previous chunk against the next one.
SELECT 'mixed parts: sorting after a read-in-order prefix';
SELECT lc FROM t_auto_lc_mixed ORDER BY id, lc LIMIT 3
SETTINGS max_threads = 1, max_block_size = 100, optimize_read_in_order = 1;

SELECT 'mixed parts: aggregation, distinct, functions';
SELECT count(), uniqExact(lc), min(lc), max(lc), sum(length(lc)) FROM t_auto_lc_mixed;
SELECT count() FROM (SELECT DISTINCT lc FROM t_auto_lc_mixed);

SELECT 'mixed parts: join and insert select';
SELECT count() FROM t_auto_lc_mixed AS l INNER JOIN t_auto_lc_mixed AS r ON l.lc = r.lc WHERE l.id < 5;

DROP TABLE IF EXISTS t_auto_lc_mixed_copy;
CREATE TABLE t_auto_lc_mixed_copy (id UInt64, lc String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_auto_lc_mixed_copy SELECT id, lc FROM t_auto_lc_mixed;
SELECT count(), uniqExact(lc) FROM t_auto_lc_mixed_copy;

SELECT 'mixed parts: merged into a single part';
SYSTEM START MERGES t_auto_lc_mixed;
OPTIMIZE TABLE t_auto_lc_mixed FINAL;
SELECT count(), uniqExact(lc), min(lc), max(lc) FROM t_auto_lc_mixed;

DROP TABLE t_auto_lc_mixed_copy;
DROP TABLE t_auto_lc_mixed;
