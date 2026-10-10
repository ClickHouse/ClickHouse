-- `parallel_full_sorting_merge` is documented to preserve `full_sorting_merge` results while only changing
-- execution parallelism. It shards both sides by the hash of the join key (`ScatterByPartitionTransform` ->
-- `IColumn::computeHashInto`, a `WeakHash32`), but the merge join matches keys with `compareAt`. For
-- `Dynamic` keys the two disagree: `ColumnDynamic::compareAt` compares the logical value, while
-- `ColumnDynamic::computeHashInto` hashes the raw variant storage - which its own comment states is "NOT
-- guaranteed to be equal for logically equal values stored with different variant layouts (typed variant vs
-- the shared variant)". So the same logical value stored as a typed variant on one side and in `shared_data`
-- on the other hashes into different shards, and hash sharding would scatter equal keys apart, making the
-- per-shard merge join miss the match and return fewer rows than `full_sorting_merge`.
--
-- A plain `Dynamic` join key is rejected earlier by `TableJoin::inferJoinKeyCommonType`, but only when
-- `allow_dynamic_type_in_join_keys = 0` (the default); with that compatibility setting enabled a `Dynamic`
-- key (or a compound key containing `Dynamic`) reaches this rewrite. The optimizer must therefore skip
-- sharding whenever a join key is (or contains) a `Dynamic` type and run a single merge join instead.
--
-- `max_threads = 4` fixes the shard count > 1 on any runner.

SET allow_dynamic_type_in_join_keys = 1;

DROP TABLE IF EXISTS pfsmj_dyn_left;
DROP TABLE IF EXISTS pfsmj_dyn_right;

-- `max_types = 1`: only one concrete type can be a typed variant, any other overflows into `shared_data`.
-- This lets the two sides store the same logical value with different physical variant layouts.
CREATE TABLE pfsmj_dyn_left  (id UInt64, k Dynamic(max_types = 1)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_dyn_right (id UInt64, k Dynamic(max_types = 1)) ENGINE = MergeTree ORDER BY id;

-- Left holds only `Int64` values, so on the left `Int64` is the single typed variant.
INSERT INTO pfsmj_dyn_left SELECT number, number::Int64 FROM numbers(100);

-- Right has the same logical `Int64` values, but its part also holds many `String` rows, so the more frequent
-- `String` grabs the single typed slot and `Int64` overflows into `shared_data` on the right. After the merge
-- the two sides store the identical `Int64` values with different variant layouts, and their hashes diverge
-- even though `compareAt` still sees them as equal.
INSERT INTO pfsmj_dyn_right SELECT number, number::Int64 FROM numbers(100);
INSERT INTO pfsmj_dyn_right SELECT 1000 + number, ('s' || toString(number))::String FROM numbers(500);
OPTIMIZE TABLE pfsmj_dyn_right FINAL;
OPTIMIZE TABLE pfsmj_dyn_left FINAL;

-- Correctness: the sharded algorithm must match `full_sorting_merge`. Without the guard the equal `Dynamic`
-- keys hash into different shards and the merge join misses matches (returns 24 rows instead of 100).
SELECT 'dynamic_key_matches_fsm',
    (SELECT (sum(l.id + r.id), count()) FROM pfsmj_dyn_left AS l INNER JOIN pfsmj_dyn_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (sum(l.id + r.id), count()) FROM pfsmj_dyn_left AS l INNER JOIN pfsmj_dyn_right AS r ON l.k = r.k SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 4);

-- The `Dynamic`-key join must NOT scatter (no `ScatterByPartitionTransform`): sharding is skipped for it.
SELECT 'dynamic_key_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_dyn_left AS l INNER JOIN pfsmj_dyn_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

DROP TABLE pfsmj_dyn_left;
DROP TABLE pfsmj_dyn_right;

DROP TABLE IF EXISTS pfsmj_ndyn_left;
DROP TABLE IF EXISTS pfsmj_ndyn_right;

-- Nested `Dynamic` inside a compound key: `Array(Dynamic)`. `joinKeyTypeBreaksHashSharding` must detect the
-- `Dynamic` inside the `Array` (via `IDataType::forEachChild`) and skip sharding just as for a top-level one.
CREATE TABLE pfsmj_ndyn_left  (id UInt64, k Array(Dynamic(max_types = 1))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_ndyn_right (id UInt64, k Array(Dynamic(max_types = 1))) ENGINE = MergeTree ORDER BY id;

INSERT INTO pfsmj_ndyn_left SELECT number, [number::Int64] FROM numbers(100);
INSERT INTO pfsmj_ndyn_right SELECT number, [number::Int64] FROM numbers(100);
INSERT INTO pfsmj_ndyn_right SELECT 1000 + number, [('s' || toString(number))::String] FROM numbers(500);
OPTIMIZE TABLE pfsmj_ndyn_right FINAL;
OPTIMIZE TABLE pfsmj_ndyn_left FINAL;

SELECT 'nested_dynamic_key_matches_fsm',
    (SELECT (sum(l.id + r.id), count()) FROM pfsmj_ndyn_left AS l INNER JOIN pfsmj_ndyn_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (sum(l.id + r.id), count()) FROM pfsmj_ndyn_left AS l INNER JOIN pfsmj_ndyn_right AS r ON l.k = r.k SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 4);

SELECT 'nested_dynamic_key_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_ndyn_left AS l INNER JOIN pfsmj_ndyn_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

DROP TABLE pfsmj_ndyn_left;
DROP TABLE pfsmj_ndyn_right;

DROP TABLE IF EXISTS pfsmj_ctl_left;
DROP TABLE IF EXISTS pfsmj_ctl_right;

-- Control: an integer join key is hash-compatible with the merge comparison, so sharding still applies. This
-- proves the fix disables sharding only for `Dynamic` (and float/JSON) keys, not for every
-- `parallel_full_sorting_merge`.
CREATE TABLE pfsmj_ctl_left  (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_ctl_right (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO pfsmj_ctl_left  SELECT number, number FROM numbers(0, 2000);
INSERT INTO pfsmj_ctl_left  SELECT number, number FROM numbers(2000, 2000);
INSERT INTO pfsmj_ctl_right SELECT number, number * 2 FROM numbers(0, 4000);

SELECT 'int_key_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_ctl_left AS l INNER JOIN pfsmj_ctl_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

DROP TABLE pfsmj_ctl_left;
DROP TABLE pfsmj_ctl_right;
