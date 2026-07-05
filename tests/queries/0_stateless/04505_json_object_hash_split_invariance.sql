-- Tags: no-fasttest
-- no-fasttest: JSON type requires the full build.

-- A logically equal JSON object must hash the same regardless of how its paths are physically
-- split between dynamic columns and shared data (the split depends on insertion history and can
-- differ across blocks / after a temp-file round-trip). ColumnObject::computeHashInto used to hash
-- the physical layout, so hash-based joins silently dropped matches for JSON keys and the grace-hash
-- spill could re-hash a key backward into an already-joined bucket, raising a FileBucket
-- "Invalid state transition" logical error.

SET allow_experimental_json_type = 1;
SET allow_dynamic_type_in_join_keys = 1;
SET allow_suspicious_types_in_group_by = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS t_dyn;
DROP TABLE IF EXISTS t_shared;
DROP TABLE IF EXISTS t_both;

-- `x` stored as a dynamic path.
CREATE TABLE t_dyn (k JSON(max_dynamic_paths = 1)) ENGINE = Memory;
INSERT INTO t_dyn VALUES ('{"x":1}');

-- Same value `{"x":1}`, but `y` fills the single dynamic slot first, so `x` lands in shared data.
CREATE TABLE t_shared (k JSON(max_dynamic_paths = 1)) ENGINE = Memory;
INSERT INTO t_shared VALUES ('{"y":9}'), ('{"x":1}');

-- Collect both physical representations of `{"x":1}` into one table.
CREATE TABLE t_both (k JSON(max_dynamic_paths = 1)) ENGINE = Memory;
INSERT INTO t_both SELECT k FROM t_dyn;
INSERT INTO t_both SELECT k FROM t_shared WHERE k = '{"x":1}'::JSON(max_dynamic_paths = 1);

-- The two rows are logically equal but physically split differently: `x` is a dynamic path in one
-- row and lives in shared data in the other. One row must have `x` as a dynamic path, the other must
-- have it in shared data.
SELECT 'splits',
       countIf(has(JSONDynamicPaths(k), 'x')),
       countIf(has(JSONSharedDataPaths(k), 'x'))
FROM t_both;

-- Equality already treats them as equal.
SELECT 'equal', (SELECT k FROM t_dyn) = (SELECT k FROM t_shared WHERE k = '{"x":1}'::JSON(max_dynamic_paths = 1));

-- Hash-based joins must match the two representations (4 = 2 rows x 2 rows).
SELECT 'hash', count() FROM t_both AS a INNER JOIN t_both AS b ON a.k = b.k SETTINGS join_algorithm = 'hash';
SELECT 'parallel_hash', count() FROM t_both AS a INNER JOIN t_both AS b ON a.k = b.k SETTINGS join_algorithm = 'parallel_hash';
SELECT 'grace_hash', count() FROM t_both AS a INNER JOIN t_both AS b ON a.k = b.k SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 4, max_block_size = 1;

-- GROUP BY and DISTINCT must collapse them into a single group.
SELECT 'group_by', count() FROM (SELECT k FROM t_both GROUP BY k);
SELECT 'distinct', count() FROM (SELECT DISTINCT k FROM t_both);
SELECT 'uniqExact', uniqExact(k) FROM t_both;

DROP TABLE t_dyn;
DROP TABLE t_shared;
DROP TABLE t_both;

-- Grace-hash spill on a JSON key whose per-block dynamic/shared split varies must not raise a
-- FileBucket state-machine assertion, and must not silently drop matches (the hash must be stable
-- across the temp-file round-trip). Spilling is forced by grace_hash_join_initial_buckets = 8
-- (independent of data volume), so a small fixture still exercises the delayed-bucket re-scatter.
-- The self-join cardinality is deterministic: the key is fully determined by number % 50 (the b
-- index (number * 7 + 1) % 50 also depends only on number % 50), so there are 50 distinct keys with
-- 20 rows each => 50 * 20 * 20 = 20000 matched pairs. Asserting the exact count catches a spill that
-- keeps one bucket alive but drops most matches, which a count() > 0 check would not. The
-- non-spilling hash join on the same data must agree.
DROP TABLE IF EXISTS t_spill;
CREATE TABLE t_spill (id UInt64, k JSON(max_dynamic_paths = 1)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1,
             object_serialization_version = 'v3',
             object_shared_data_serialization_version = 'advanced',
             object_shared_data_serialization_version_for_zero_level_parts = 'advanced';
INSERT INTO t_spill SELECT number,
    concat('{"a', toString(number % 50), '":1, "b', toString((number * 7 + 1) % 50), '":2}')::JSON(max_dynamic_paths = 1)
FROM numbers(1000);

SELECT 'spill_join_grace', count()
FROM t_spill AS a INNER JOIN t_spill AS b ON a.k = b.k
SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 8, max_block_size = 11, max_joined_block_size_rows = 1;

SELECT 'spill_join_hash', count()
FROM t_spill AS a INNER JOIN t_spill AS b ON a.k = b.k
SETTINGS join_algorithm = 'hash';

DROP TABLE t_spill;
