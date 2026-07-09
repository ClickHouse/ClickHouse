SET explain_query_plan_default = 'legacy';

-- { echo }

-- Deterministic key-transform atoms must be produced for the key columns that the
-- direct match does not cover, not only when nothing else matched. For example, with
-- `ORDER BY (concat(s, '_x'), s)` and `WHERE s = 'b'`, the direct atom covers `s`,
-- and the deterministic transform must additionally constrain the leading key column
-- `concat(s, '_x')`.

-- An injective wrap of the direct column leads the key: both atoms must be exact.
DROP TABLE IF EXISTS test_det_injective;
CREATE TABLE test_det_injective (s String) ENGINE = MergeTree
ORDER BY (concat(s, '_x'), s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_det_injective SELECT char(97 + intDiv(number, 4)) FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_det_injective WHERE s = 'b') WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_det_injective WHERE s = 'b' SETTINGS force_primary_key = 1;
SELECT count() FROM test_det_injective WHERE s = 'b' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_det_injective WHERE s != 'b') WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_det_injective WHERE s != 'b' SETTINGS force_primary_key = 1;
SELECT count() FROM test_det_injective WHERE s != 'b' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_det_injective;

-- A non-injective wrap leads the key: the extra atom is relaxed, still prunes for
-- equality, and must never prune wrongly.
DROP TABLE IF EXISTS test_det_noninjective;
CREATE TABLE test_det_noninjective (s String) ENGINE = MergeTree
ORDER BY (cityHash64(s) % 8, s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_det_noninjective SELECT char(97 + intDiv(number, 4)) FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_det_noninjective WHERE s = 'b') WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_det_noninjective WHERE s = 'b';
SELECT count() FROM test_det_noninjective WHERE s = 'b' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

-- The relaxed atom of a non-injective transform never prunes for `notEquals`.
SELECT count() FROM test_det_noninjective WHERE s != 'b';
SELECT count() FROM test_det_noninjective WHERE s != 'b' SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_det_noninjective;

-- A plain single-column key must keep its single direct atom: the deterministic pass
-- must not add anything through the column's identity transform.
DROP TABLE IF EXISTS test_det_plain;
CREATE TABLE test_det_plain (id UInt64) ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_det_plain SELECT number FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_det_plain WHERE id = 5) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_det_plain WHERE id = 5 SETTINGS force_primary_key = 1;
SELECT count() FROM test_det_plain WHERE id = 5 SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0;

DROP TABLE test_det_plain;
