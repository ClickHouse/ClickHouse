-- Tags: no-fasttest
-- no-fasttest: JSON type requires the full build.

-- A logically equal JSON object must hash the same regardless of how its paths are physically
-- split between dynamic columns and shared data (the split depends on insertion history and can
-- differ across blocks / after a temp-file round-trip). ColumnObject::computeHashInto used to hash
-- the physical layout, so hash-based joins silently dropped matches for JSON keys and the grace-hash
-- spill could re-hash a key backward into an already-joined bucket, raising a FileBucket
-- "Invalid state transition" logical error.

SET allow_experimental_json_type = 1;
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

-- The cases above have a single path, so the object's whole value moves between the dynamic and the
-- shared section together. That is not enough to pin `ColumnObject::computeHashInto` itself: with a
-- single path, chaining the sub-columns in physical order happens to agree with the canonical order,
-- because the nested `ColumnDynamic` (fixed separately below) carries the whole difference. With TWO
-- paths the sections are populated in both rows and the physical order genuinely diverges from sorted
-- path order: `{"a":1,"b":2}` keeps `a` dynamic and `b` shared in one block, and `b` dynamic with `a`
-- shared in the other. Both rows are logically equal, so the self-join must match all 4 pairs; the
-- pre-fix hash matches only the 2 self-pairs.
DROP TABLE IF EXISTS t2_a;
DROP TABLE IF EXISTS t2_b;
DROP TABLE IF EXISTS t2_both;
CREATE TABLE t2_a (k JSON(max_dynamic_paths = 1)) ENGINE = Memory;
INSERT INTO t2_a VALUES ('{"a":1,"b":2}');
CREATE TABLE t2_b (id UInt8, k JSON(max_dynamic_paths = 1)) ENGINE = Memory;
INSERT INTO t2_b VALUES (1, '{"b":2}'), (2, '{"a":1,"b":2}');
CREATE TABLE t2_both (k JSON(max_dynamic_paths = 1)) ENGINE = Memory;
INSERT INTO t2_both SELECT k FROM t2_a;
INSERT INTO t2_both SELECT k FROM t2_b WHERE id = 2;

-- Each row has one path dynamic and the other shared, and the two rows disagree on which is which.
SELECT 'two_path_splits',
       countIf(has(JSONDynamicPaths(k), 'a')),
       countIf(has(JSONSharedDataPaths(k), 'a'))
FROM t2_both;

SELECT 'two_path_hash', count() FROM t2_both AS a INNER JOIN t2_both AS b ON a.k = b.k SETTINGS join_algorithm = 'hash';
-- 8 buckets, not 4: with 4 the two pre-fix hashes happen to land in the same bucket and the join
-- still matches, so 4 buckets would not catch a regression in `ColumnObject::computeHashInto`.
SELECT 'two_path_grace_hash', count() FROM t2_both AS a INNER JOIN t2_both AS b ON a.k = b.k SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 8, max_block_size = 1;
SELECT 'two_path_group_by', count() FROM (SELECT k FROM t2_both GROUP BY k);

DROP TABLE t2_a;
DROP TABLE t2_b;
DROP TABLE t2_both;

-- Grace-hash spill on a JSON key whose per-block dynamic/shared split varies must not raise a
-- FileBucket state-machine assertion, and must not silently drop matches (the hash must be stable
-- across the temp-file round-trip). Spilling to disk is forced by grace_hash_join_initial_buckets = 8
-- (independent of data volume): the right side is scattered by key hash across 8 buckets and buckets
-- 1..7 are written to temp files and read back, so a small fixture still exercises the full spill
-- round-trip.
--
-- Both paths are named `a` and `b` in every row, and each of the two parts is seeded with a row that
-- claims the single dynamic slot for the OTHER path, so the same logical key is stored with `a`
-- dynamic in one part and `b` dynamic in the other -- the `t2_both` shape above, at spill scale. The
-- seeder rows are excluded from the join, and `spill_splits` asserts the divergence is actually
-- present: without it a background merge that unified the layout would silently turn the whole cell
-- into a same-representation smoke test that still returned the right count.
--
-- The self-join cardinality is deterministic: the key is fully determined by number % 20 (the `b`
-- value (number * 7 + 1) % 20 also depends only on number % 20), so there are 20 distinct keys with
-- 5 rows per part => 20 * 10 * 10 = 2000 matched pairs. Asserting the exact count catches a spill
-- that keeps one bucket alive but drops most matches, which a count() > 0 check would not. The
-- non-spilling hash join on the same data must agree. joined_block_split_single_row is pinned off:
-- it forces one output row per block and does not affect the build-side scatter this test exercises,
-- so leaving it randomized only makes the run slow.
DROP TABLE IF EXISTS t_spill;
CREATE TABLE t_spill (id UInt64, k JSON(max_dynamic_paths = 1)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1,
             object_serialization_version = 'v3',
             object_shared_data_serialization_version = 'advanced',
             object_shared_data_serialization_version_for_zero_level_parts = 'advanced';
-- Part 1: `b` takes the dynamic slot, so every row keeps `a` in shared data.
INSERT INTO t_spill SELECT number,
    if(number = 0, '{"b":999}',
       concat('{"a":', toString(number % 20), ', "b":', toString((number * 7 + 1) % 20), '}'))::JSON(max_dynamic_paths = 1)
FROM numbers(101);
-- Part 2: the same keys with the split reversed.
INSERT INTO t_spill SELECT number + 1000,
    if(number = 0, '{"a":999}',
       concat('{"a":', toString(number % 20), ', "b":', toString((number * 7 + 1) % 20), '}'))::JSON(max_dynamic_paths = 1)
FROM numbers(101);

-- The two halves must disagree on which path is dynamic (100 rows each way, seeders excluded).
SELECT 'spill_splits',
       countIf(id < 1000 AND has(JSONDynamicPaths(k), 'b')),
       countIf(id > 1000 AND has(JSONDynamicPaths(k), 'a'))
FROM t_spill WHERE id % 1000 != 0;

SELECT 'spill_join_grace', count()
FROM (SELECT k FROM t_spill WHERE id % 1000 != 0) AS a INNER JOIN (SELECT k FROM t_spill WHERE id % 1000 != 0) AS b ON a.k = b.k
SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 8, max_block_size = 11, joined_block_split_single_row = 0,
         log_comment = '04505_spill_json';

SELECT 'spill_join_hash', count()
FROM (SELECT k FROM t_spill WHERE id % 1000 != 0) AS a INNER JOIN (SELECT k FROM t_spill WHERE id % 1000 != 0) AS b ON a.k = b.k
SETTINGS join_algorithm = 'hash';

DROP TABLE t_spill;

-- The same invariant must hold for plain `Dynamic` keys. `compareAt` already treats a value in a
-- typed variant and the same value in the shared variant as equal, but ColumnDynamic::computeHashInto
-- used to forward to the raw ColumnVariant layout (discriminator + typed offset), so a value that
-- lands in the shared variant in one row and a typed variant in another hashed differently. The
-- `grace_hash` scatter and its temp-file spill both rely on that hash, so direct `Dynamic` join keys
-- silently dropped matches (and a backward re-hash after a spill risks the FileBucket assertion).

SET allow_experimental_dynamic_type = 1;
-- A bare `Dynamic` join key requires this; a `JSON` key does not, which is why the JSON cases above
-- run without it. `DataTypeObject::forEachChild` visits only typed paths, so `hasDynamicType` never
-- sees a `JSON` column's inner `Dynamic` and the join-key gates do not fire.
SET allow_dynamic_type_in_join_keys = 1;

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

-- The cases above all use an `Int64` value, which is the one width where hashing the shared variant's
-- serialized blob happens to agree with hashing the typed column's representation: the typed hash
-- zero-extends the value into a 64-bit word, while a raw-bytes hash of a shorter value mixes the byte
-- COUNT into the word instead. So an `Int64` fixture cannot tell the decoded-value hash apart from a
-- raw-blob hash. `String` (whose blob carries a varint length prefix that the typed hash never sees)
-- and any numeric narrower than 8 bytes do distinguish them, so both are covered here.
DROP TABLE IF EXISTS d_str_typed;
DROP TABLE IF EXISTS d_str_shared;
DROP TABLE IF EXISTS d_str_both;
CREATE TABLE d_str_typed (k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO d_str_typed VALUES ('abcdefghijk'::String);
CREATE TABLE d_str_shared (k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO d_str_shared VALUES (7::Int64), ('abcdefghijk'::String);
CREATE TABLE d_str_both (k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO d_str_both SELECT k FROM d_str_typed;
INSERT INTO d_str_both SELECT k FROM d_str_shared WHERE dynamicType(k) = 'String';

SELECT 'dyn_str_splits',
       countIf(NOT isDynamicElementInSharedData(k)),
       countIf(isDynamicElementInSharedData(k))
FROM d_str_both;
SELECT 'dyn_str_grace_hash', count() FROM d_str_both AS a INNER JOIN d_str_both AS b ON a.k = b.k SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 4, max_block_size = 1;

DROP TABLE IF EXISTS d_u8_typed;
DROP TABLE IF EXISTS d_u8_shared;
DROP TABLE IF EXISTS d_u8_both;
CREATE TABLE d_u8_typed (k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO d_u8_typed VALUES (5::UInt8);
CREATE TABLE d_u8_shared (k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO d_u8_shared VALUES ('fill'::String), (5::UInt8);
CREATE TABLE d_u8_both (k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO d_u8_both SELECT k FROM d_u8_typed;
INSERT INTO d_u8_both SELECT k FROM d_u8_shared WHERE dynamicType(k) = 'UInt8';

SELECT 'dyn_u8_splits',
       countIf(NOT isDynamicElementInSharedData(k)),
       countIf(isDynamicElementInSharedData(k))
FROM d_u8_both;
SELECT 'dyn_u8_grace_hash', count() FROM d_u8_both AS a INNER JOIN d_u8_both AS b ON a.k = b.k SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 4, max_block_size = 1;

DROP TABLE d_str_typed;
DROP TABLE d_str_shared;
DROP TABLE d_str_both;
DROP TABLE d_u8_typed;
DROP TABLE d_u8_shared;
DROP TABLE d_u8_both;

-- Grace-hash spill on a `Dynamic` key whose typed/shared split varies must not drop matches (the hash
-- must be stable across the temp-file round-trip) and must not raise a FileBucket state-machine
-- assertion. `s_typed` stores every key as a typed `Int64`; `s_shared` puts a String first so every
-- `Int64` lands in the shared variant. Merged into one table, each key value 0..24 appears 8 times
-- (4 typed + 4 shared), so the self-join is deterministic: 25 * 8 * 8 = 1600 matched pairs. Disk
-- spilling is forced by grace_hash_join_initial_buckets = 8 regardless of data volume, so this small
-- fixture still exercises the full spill round-trip; joined_block_split_single_row is pinned off (see
-- the JSON spill above). This is the case the pre-fix hash gets wrong: the typed and shared copies of
-- the same value scatter into different buckets, so the spilled build side drops most matches.
DROP TABLE IF EXISTS s_typed;
DROP TABLE IF EXISTS s_shared;
DROP TABLE IF EXISTS d_spill;
CREATE TABLE s_typed (n UInt64, k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO s_typed SELECT number, (number % 25)::Int64 FROM numbers(100);
CREATE TABLE s_shared (n UInt64, k Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO s_shared SELECT number, if(number = 0, 'fill'::Dynamic(max_types = 1), ((number - 1) % 25)::Int64::Dynamic(max_types = 1)) FROM numbers(101);
CREATE TABLE d_spill (n UInt64, k Dynamic(max_types = 1)) ENGINE = MergeTree ORDER BY n
    SETTINGS min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;
INSERT INTO d_spill SELECT n, k FROM s_typed;
INSERT INTO d_spill SELECT n + 1000, k FROM s_shared WHERE dynamicType(k) = 'Int64';

-- Guard the layout this spill case depends on, the way `spill_splits` does for the JSON one. The
-- `dyn_splits` cell above inspects `d_both`, a Memory table; `d_spill` is a MergeTree, so its
-- typed/shared split additionally survives part serialization and read-back. Without this row, a
-- change that normalized every row to one representation would leave the counts at 1600 and the cell
-- would stay green while covering nothing.
SELECT 'dyn_spill_splits',
       countIf(n < 1000 AND NOT isDynamicElementInSharedData(k)),
       countIf(n > 1000 AND isDynamicElementInSharedData(k))
FROM d_spill;

SELECT 'dyn_spill_grace', count()
FROM d_spill AS a INNER JOIN d_spill AS b ON a.k = b.k
SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 8, max_block_size = 11, joined_block_split_single_row = 0,
         log_comment = '04505_spill_dyn';

SELECT 'dyn_spill_hash', count()
FROM d_spill AS a INNER JOIN d_spill AS b ON a.k = b.k
SETTINGS join_algorithm = 'hash';

-- Both spill cases above assert only a cardinality, which a run that never touched disk satisfies
-- just as well: GraceHashJoin::flushBlocksToBuckets skips bucket 0 by construction and skips empty
-- buckets, so "grace_hash was requested" does not imply a temp file was written. The FileBucket
-- state-machine half of what these cells cover is only exercised if one was, so assert the write
-- directly.
--
-- ExternalJoinWritePart alone does not do that: GraceHashJoin::addBuckets constructs both temp streams
-- of every bucket up front and each holder's constructor already increments that counter, so it counts
-- buckets rather than payload. Running these very two joins over an empty row set still reports
-- ExternalJoinWritePart = 16 -- on zero payload bytes. So pair it with a byte threshold, the way
-- 02402_external_disk_metrics does.
--
-- ExternalJoinUncompressedBytes is the counter to assert: it measures payload before compression, so
-- unlike the compressed counter it does not move with temporary_files_codec. Measured over 40 runs
-- under the test runner's own setting randomization, it stays in 12575..15305 bytes for the JSON join
-- and 4485..7513 for the Dynamic one, so 2000 leaves better than a factor of two of headroom below the
-- lowest draw while being unreachable without a real flush: bucket construction alone contributes 0,
-- and keeping everything in the in-memory bucket (grace_hash_join_initial_buckets = 1) also leaves it
-- at exactly 0. The counters are per query, so the threshold applies to both cells.
--
-- currentDatabase() alone does not isolate this execution: with --database the runner reuses one
-- database for every test, so aggregating the whole log lets an earlier spilling run keep a later
-- non-spilling one green. Both counters come out of one argMax tuple so they cannot be read from
-- different executions.
SYSTEM FLUSH LOGS query_log;
SELECT 'spilled', log_comment,
       if(last.1 >= 1 AND last.2 >= 2000,
          'ok',
          'fail: write_part=' || toString(last.1) || ' uncompressed=' || toString(last.2))
FROM (
    SELECT log_comment,
           argMax((ProfileEvents['ExternalJoinWritePart'], ProfileEvents['ExternalJoinUncompressedBytes']),
                  event_time_microseconds) AS last
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment LIKE '04505_spill%' AND type = 'QueryFinish'
    GROUP BY log_comment
)
ORDER BY log_comment;

DROP TABLE s_typed;
DROP TABLE s_shared;
DROP TABLE d_spill;
