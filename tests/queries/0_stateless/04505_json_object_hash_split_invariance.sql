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

-- The same invariant must hold for plain `Dynamic` keys. `compareAt` already treats a value in a
-- typed variant and the same value in the shared variant as equal, but ColumnDynamic::computeHashInto
-- used to forward to the raw ColumnVariant layout (discriminator + typed offset), so a value that
-- lands in the shared variant in one row and a typed variant in another hashed differently. The
-- `grace_hash` scatter and its temp-file spill both rely on that hash, so direct `Dynamic` join keys
-- silently dropped matches (and a backward re-hash after a spill risks the FileBucket assertion).

SET allow_experimental_dynamic_type = 1;

DROP TABLE IF EXISTS d_typed;
DROP TABLE IF EXISTS d_shared;
DROP TABLE IF EXISTS d_both;

-- `1::Int64` stored as a typed variant (Int64 fits the single dynamic slot).
CREATE TABLE d_typed (k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO d_typed VALUES (1::Int64);

-- Same value `1::Int64`, but a String fills the single dynamic slot first, so `Int64` lands in the
-- shared variant.
CREATE TABLE d_shared (k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO d_shared VALUES ('fillstr'), (1::Int64);

-- Collect both physical representations of `1::Int64` into one table.
CREATE TABLE d_both (k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO d_both SELECT k FROM d_typed;
INSERT INTO d_both SELECT k FROM d_shared WHERE dynamicType(k) = 'Int64';

-- One row keeps `Int64` as a typed variant, the other has it in the shared variant.
SELECT 'dyn_splits',
       countIf(NOT isDynamicElementInSharedData(k)),
       countIf(isDynamicElementInSharedData(k))
FROM d_both;

-- Equality already treats them as equal.
SELECT 'dyn_equal', (SELECT k FROM d_typed) = (SELECT k FROM d_shared WHERE dynamicType(k) = 'Int64');

-- Hash-based joins must match the two representations (4 = 2 rows x 2 rows).
SELECT 'dyn_hash', count() FROM d_both AS a INNER JOIN d_both AS b ON a.k = b.k SETTINGS join_algorithm = 'hash';
SELECT 'dyn_parallel_hash', count() FROM d_both AS a INNER JOIN d_both AS b ON a.k = b.k SETTINGS join_algorithm = 'parallel_hash';
SELECT 'dyn_grace_hash', count() FROM d_both AS a INNER JOIN d_both AS b ON a.k = b.k SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 4, max_block_size = 1;

-- GROUP BY and DISTINCT must collapse them into a single group.
SELECT 'dyn_group_by', count() FROM (SELECT k FROM d_both GROUP BY k);
SELECT 'dyn_distinct', count() FROM (SELECT DISTINCT k FROM d_both);
SELECT 'dyn_uniqExact', uniqExact(k) FROM d_both;

DROP TABLE d_typed;
DROP TABLE d_shared;
DROP TABLE d_both;

-- Grace-hash spill on a `Dynamic` key whose typed/shared split varies must not drop matches (the hash
-- must be stable across the temp-file round-trip) and must not raise a FileBucket state-machine
-- assertion. `s_typed` stores every key as a typed `Int64`; `s_shared` puts a String first so every
-- `Int64` lands in the shared variant. Merged into one table, each key value 0..49 appears 20 times
-- (10 typed + 10 shared), so the self-join is deterministic: 50 * 20 * 20 = 20000 matched pairs. The
-- non-spilling hash join on the same data must agree.
DROP TABLE IF EXISTS s_typed;
DROP TABLE IF EXISTS s_shared;
DROP TABLE IF EXISTS d_spill;
CREATE TABLE s_typed (n UInt64, k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO s_typed SELECT number, (number % 50)::Int64 FROM numbers(500);
CREATE TABLE s_shared (n UInt64, k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO s_shared SELECT number, if(number = 0, 'fill'::Dynamic(max_types = 1), ((number - 1) % 50)::Int64::Dynamic(max_types = 1)) FROM numbers(501);
CREATE TABLE d_spill (n UInt64, k Dynamic(max_types = 1)) ENGINE = MergeTree ORDER BY n
    SETTINGS min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;
INSERT INTO d_spill SELECT n, k FROM s_typed;
INSERT INTO d_spill SELECT n + 1000, k FROM s_shared WHERE dynamicType(k) = 'Int64';

SELECT 'dyn_spill_grace', count()
FROM d_spill AS a INNER JOIN d_spill AS b ON a.k = b.k
SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 8, max_block_size = 11, max_joined_block_size_rows = 1;

SELECT 'dyn_spill_hash', count()
FROM d_spill AS a INNER JOIN d_spill AS b ON a.k = b.k
SETTINGS join_algorithm = 'hash';

DROP TABLE s_typed;
DROP TABLE s_shared;
DROP TABLE d_spill;
