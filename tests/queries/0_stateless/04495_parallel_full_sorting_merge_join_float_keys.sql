-- `parallel_full_sorting_merge` is documented to preserve `full_sorting_merge` results while only changing
-- execution parallelism. It shards both sides by the hash of the join key (`ScatterByPartitionTransform` ->
-- `IColumn::computeHashInto`), but the merge join matches keys with `compareAt`. For floating-point keys the
-- two disagree: `-0.0` and `+0.0` compare equal for the merge but hash differently, so hash sharding would
-- scatter them into different shards and miss the match, returning fewer rows than `full_sorting_merge`.
-- The optimizer must therefore skip sharding whenever a join key is (or contains) a floating-point type and
-- run a single merge join instead.
--
-- The reference is `full_sorting_merge`, NOT `hash`: `hash` does not treat `-0.0` and `+0.0` as equal, so it
-- would give a different (unrelated) answer. `max_threads = 4` fixes the shard count > 1 on any runner.

DROP TABLE IF EXISTS pfsmj_float_left;
DROP TABLE IF EXISTS pfsmj_float_right;

-- The join is on `k` (a `Float64`), not on the tables' `ORDER BY` key, so a plain full sort is built - the
-- case the scatter path would rewrite.
CREATE TABLE pfsmj_float_left (id UInt64, k Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_float_right (id UInt64, k Float64) ENGINE = MergeTree ORDER BY id;

-- Even rows carry signed zero (`-0.0` on the left, `+0.0` on the right): equal for the merge, different hash.
-- Odd rows carry ordinary distinct floats that match normally. Two parts per side for multiple streams.
INSERT INTO pfsmj_float_left  SELECT number, if(number % 2 = 0, -0.0, toFloat64(number)) FROM numbers(0, 2000);
INSERT INTO pfsmj_float_left  SELECT number, if(number % 2 = 0, -0.0, toFloat64(number)) FROM numbers(2000, 2000);
INSERT INTO pfsmj_float_right SELECT number, if(number % 2 = 0,  0.0, toFloat64(number)) FROM numbers(0, 2000);
INSERT INTO pfsmj_float_right SELECT number, if(number % 2 = 0,  0.0, toFloat64(number)) FROM numbers(2000, 2000);

-- Correctness: the sharded algorithm must match `full_sorting_merge` (the signed-zero rows must still join).
SELECT 'float_key_matches_fsm',
    (SELECT (sum(l.id + r.id), count()) FROM pfsmj_float_left AS l INNER JOIN pfsmj_float_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (sum(l.id + r.id), count()) FROM pfsmj_float_left AS l INNER JOIN pfsmj_float_right AS r ON l.k = r.k SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 4);

-- The float-key join must NOT scatter (no `ScatterByPartitionTransform`): sharding is skipped for float keys.
SELECT 'float_key_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_float_left AS l INNER JOIN pfsmj_float_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

DROP TABLE pfsmj_float_left;
DROP TABLE pfsmj_float_right;

DROP TABLE IF EXISTS pfsmj_nfloat_left;
DROP TABLE IF EXISTS pfsmj_nfloat_right;

-- A float nested inside `Nullable` must also disable sharding (the type check recurses through wrappers).
CREATE TABLE pfsmj_nfloat_left (id UInt64, k Nullable(Float64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_nfloat_right (id UInt64, k Nullable(Float64)) ENGINE = MergeTree ORDER BY id;

INSERT INTO pfsmj_nfloat_left  SELECT number, if(number % 3 = 0, NULL, if(number % 2 = 0, -0.0, toFloat64(number))) FROM numbers(0, 2000);
INSERT INTO pfsmj_nfloat_left  SELECT number, if(number % 3 = 0, NULL, if(number % 2 = 0, -0.0, toFloat64(number))) FROM numbers(2000, 2000);
INSERT INTO pfsmj_nfloat_right SELECT number, if(number % 3 = 0, NULL, if(number % 2 = 0,  0.0, toFloat64(number))) FROM numbers(0, 2000);
INSERT INTO pfsmj_nfloat_right SELECT number, if(number % 3 = 0, NULL, if(number % 2 = 0,  0.0, toFloat64(number))) FROM numbers(2000, 2000);

SELECT 'nullable_float_key_matches_fsm',
    (SELECT (sum(l.id + r.id), count()) FROM pfsmj_nfloat_left AS l INNER JOIN pfsmj_nfloat_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (sum(l.id + r.id), count()) FROM pfsmj_nfloat_left AS l INNER JOIN pfsmj_nfloat_right AS r ON l.k = r.k SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 4);

SELECT 'nullable_float_key_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_nfloat_left AS l INNER JOIN pfsmj_nfloat_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

DROP TABLE pfsmj_nfloat_left;
DROP TABLE pfsmj_nfloat_right;

DROP TABLE IF EXISTS pfsmj_int_left;
DROP TABLE IF EXISTS pfsmj_int_right;

-- Control: an integer join key is hash-compatible with the merge comparison, so sharding still applies.
-- This proves the fix disables sharding only for float keys, not for every `parallel_full_sorting_merge`.
CREATE TABLE pfsmj_int_left (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_int_right (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO pfsmj_int_left  SELECT number, number FROM numbers(0, 2000);
INSERT INTO pfsmj_int_left  SELECT number, number FROM numbers(2000, 2000);
INSERT INTO pfsmj_int_right SELECT number, number * 2 FROM numbers(0, 4000);

SELECT 'int_key_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_int_left AS l INNER JOIN pfsmj_int_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

DROP TABLE pfsmj_int_left;
DROP TABLE pfsmj_int_right;
