-- `parallel_full_sorting_merge` is documented to preserve `full_sorting_merge` results while only changing
-- execution parallelism. It shards both sides by the hash of the join key (`ScatterByPartitionTransform` ->
-- `IColumn::computeHashInto`, a `WeakHash32`), but the merge join matches keys with `compareAt`. For
-- `Object('json')` / `JSON` keys the two disagree: `ColumnObject::compareAt` compares the logical path/value
-- map, while `ColumnObject::computeHashInto` hashes the physical layout - whether a path is stored as a
-- typed/dynamic subcolumn or serialized into `shared_data` (the method's own comment states it "does NOT
-- guarantee equal hashes for a logically equal object whose paths are split differently"). The same logical
-- object stored with different layouts on the two sides hashes into different shards, so hash sharding would
-- scatter equal keys apart and the per-shard merge join would miss the match, returning fewer rows than
-- `full_sorting_merge`. The optimizer must therefore skip sharding whenever a join key is (or contains) a
-- JSON/Object type and run a single merge join instead.
--
-- `max_threads = 4` fixes the shard count > 1 on any runner.

DROP TABLE IF EXISTS pfsmj_json_left;
DROP TABLE IF EXISTS pfsmj_json_right;

-- `max_dynamic_paths = 1`: only one JSON path can be a dynamic subcolumn, any other path overflows into
-- `shared_data`. This lets the two sides store the same logical object with different physical layouts.
CREATE TABLE pfsmj_json_left  (id UInt64, k JSON(max_dynamic_paths = 1)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_json_right (id UInt64, k JSON(max_dynamic_paths = 1)) ENGINE = MergeTree ORDER BY id;

-- Left rows are pure `{"a": v}`, so on the left `a` is the single dynamic subcolumn.
INSERT INTO pfsmj_json_left SELECT number, concat('{"a": ', toString(number % 5), '}') FROM numbers(100);

-- Right has the same logical `{"a": v}` rows, but its part also holds many `{"z": ...}` rows, so the more
-- frequent path `z` grabs the single dynamic slot and `a` overflows into `shared_data` on the right. After
-- the merge the two sides store the identical objects with different physical layouts, and their hashes
-- diverge even though `compareAt` still sees them as equal.
INSERT INTO pfsmj_json_right SELECT number, concat('{"a": ', toString(number % 5), '}') FROM numbers(100);
INSERT INTO pfsmj_json_right SELECT 1000 + number, concat('{"z": ', toString(number), '}') FROM numbers(500);
OPTIMIZE TABLE pfsmj_json_right FINAL;
OPTIMIZE TABLE pfsmj_json_left FINAL;

-- Correctness: the sharded algorithm must match `full_sorting_merge`. Without the guard the equal JSON keys
-- hash into different shards and the merge join misses every match (returns 0 rows instead of 2000).
SELECT 'json_key_matches_fsm',
    (SELECT (sum(l.id + r.id), count()) FROM pfsmj_json_left AS l INNER JOIN pfsmj_json_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (sum(l.id + r.id), count()) FROM pfsmj_json_left AS l INNER JOIN pfsmj_json_right AS r ON l.k = r.k SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 4);

-- The JSON-key join must NOT scatter (no `ScatterByPartitionTransform`): sharding is skipped for JSON keys.
SELECT 'json_key_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_json_left AS l INNER JOIN pfsmj_json_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

DROP TABLE pfsmj_json_left;
DROP TABLE pfsmj_json_right;

DROP TABLE IF EXISTS pfsmj_ctl_left;
DROP TABLE IF EXISTS pfsmj_ctl_right;

-- Control: an integer join key is hash-compatible with the merge comparison, so sharding still applies. This
-- proves the fix disables sharding only for JSON (and float) keys, not for every `parallel_full_sorting_merge`.
CREATE TABLE pfsmj_ctl_left  (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_ctl_right (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO pfsmj_ctl_left  SELECT number, number FROM numbers(0, 2000);
INSERT INTO pfsmj_ctl_left  SELECT number, number FROM numbers(2000, 2000);
INSERT INTO pfsmj_ctl_right SELECT number, number * 2 FROM numbers(0, 4000);

SELECT 'int_key_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE SELECT l.id FROM pfsmj_ctl_left AS l INNER JOIN pfsmj_ctl_right AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

DROP TABLE pfsmj_ctl_left;
DROP TABLE pfsmj_ctl_right;
