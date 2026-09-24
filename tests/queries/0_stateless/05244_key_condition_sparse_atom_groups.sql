-- Tags: no-random-settings, no-random-merge-tree-settings
-- The selected-granule and exact-range checks require fixed index and query-plan settings.

SET explain_query_plan_default = 'legacy';
SET analyze_index_with_multiple_key_columns_per_condition = 1;
SET use_lightweight_primary_key_index_analysis = 1;
SET use_query_condition_cache = 0;

-- The first part retains only `id` in its primary index. The second retains `id` and `x`.
-- The same condition must preserve pruning through `x` wherever that column is available.
CREATE TABLE sparse_atom_parts (id UInt64, x UInt64) ENGINE = MergeTree
ORDER BY (id, x, x + 1, x % 3)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9,
    add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES sparse_atom_parts;
INSERT INTO sparse_atom_parts SELECT number, number % 8 FROM numbers(64);
INSERT INTO sparse_atom_parts SELECT 100, number FROM numbers(64);

SELECT 'Mixed index layouts';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(id) FROM sparse_atom_parts WHERE (id >= 16 AND id < 32 OR id = 100) AND x = 5)
WHERE explain LIKE '%Granules%';
SELECT count(), sum(id) FROM sparse_atom_parts WHERE (id >= 16 AND id < 32 OR id = 100) AND x = 5;

-- An unavailable comparison stays unknown under disjunction and negation. It cannot certify
-- that a whole granule matches, even when its sibling comparisons refer to the same predicate.
SELECT 'Disjunction and negation';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(id) FROM sparse_atom_parts WHERE id < 4 OR x = 5)
WHERE explain LIKE '%Granules%';
SELECT count(), sum(id) FROM sparse_atom_parts WHERE id < 4 OR x = 5;
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(id) FROM sparse_atom_parts WHERE NOT (id < 4 AND x = 5))
WHERE explain LIKE '%Granules%';
SELECT count(), sum(id) FROM sparse_atom_parts WHERE NOT (id < 4 AND x = 5);
SELECT count() FROM sparse_atom_parts WHERE id >= 16 AND id < 32 AND x = 5
SETTINGS log_comment = '05244 unavailable comparisons';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows > 2 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05244 unavailable comparisons' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP TABLE sparse_atom_parts;

-- A retained derived column must still improve pruning over the direct predicate alone.
CREATE TABLE sparse_atom_retained (id UInt64, x UInt16) ENGINE = MergeTree
ORDER BY (id, toUInt8(x), x)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO sparse_atom_retained SELECT 0, number FROM numbers(512);

SELECT 'Retained derived column';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(x) FROM sparse_atom_retained WHERE id = 0 AND x = 257
SETTINGS analyze_index_with_multiple_key_columns_per_condition = 0)
WHERE explain LIKE '%Granules%';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(x) FROM sparse_atom_retained WHERE id = 0 AND x = 257)
WHERE explain LIKE '%Granules%';
SELECT count(), sum(x) FROM sparse_atom_retained WHERE id = 0 AND x = 257;
DROP TABLE sparse_atom_retained;

-- Although the primary index drops the suffix, the partition bound on `x` remains available.
-- The disjunction keeps both parts, so primary-key analysis must apply each part's bound.
CREATE TABLE sparse_atom_partition (id UInt64, x UInt16) ENGINE = MergeTree
PARTITION BY x ORDER BY (id, toUInt8(x), x)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO sparse_atom_partition SELECT number, 1 FROM numbers(32);
INSERT INTO sparse_atom_partition SELECT number, 257 FROM numbers(32);

SET use_partition_pruning = 0;
SELECT 'Partition-bounded suffix';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(id) FROM sparse_atom_partition WHERE (id >= 8 AND id < 16 AND x = 257) OR (id >= 24 AND id < 28 AND x = 1)
SETTINGS use_partition_minmax_for_primary_key_pruning = 0)
WHERE explain LIKE '%Granules%';
SELECT trimLeft(explain) FROM
(EXPLAIN indexes = 1 SELECT sum(id) FROM sparse_atom_partition WHERE (id >= 8 AND id < 16 AND x = 257) OR (id >= 24 AND id < 28 AND x = 1)
SETTINGS use_partition_minmax_for_primary_key_pruning = 1)
WHERE explain LIKE '%Granules%';
SELECT count(), sum(id) FROM sparse_atom_partition WHERE (id >= 8 AND id < 16 AND x = 257) OR (id >= 24 AND id < 28 AND x = 1);
SELECT count() FROM sparse_atom_partition WHERE x = 257
SETTINGS use_partition_minmax_for_primary_key_pruning = 1, log_comment = '05244 bounded comparisons';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows <= 2 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05244 bounded comparisons' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP TABLE sparse_atom_partition;
