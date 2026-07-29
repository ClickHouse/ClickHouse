-- A part whose column type legitimately lags the storage type (metadata-only `T` -> `Nullable(T)`
-- and Variant-extension ALTERs skip the data rewrite) must be recorded with the type the mutation
-- actually writes: the pipeline/storage type when the mutation re-serializes every column, the
-- source part's type when it carries the column's files over unchanged.
--
-- `enable_block_number_column` is what selects the full-rewrite mode here: it appends a
-- `READ_COLUMN _block_number` command, which makes the interpreter's last stage cover every
-- storage column, so `isAffectingAllColumns()` holds.
--
-- `has(substreams, ...)` rather than the whole list because `string_serialization_version` is
-- randomized and decides whether a `.size` substream exists; the `.null` one is what the corrected
-- type requires.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

SELECT '-- String, Wide: part type follows the full rewrite';

DROP TABLE IF EXISTS t_full_rewrite_str;
CREATE TABLE t_full_rewrite_str (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         -- `tdigest` is not supported for String, so the column ends up with no statistics, which
         -- is what lets `canSkipConversionToNullable` take the metadata-only path below.
         auto_statistics_types = 'tdigest',
         enable_block_number_column = true;

INSERT INTO t_full_rewrite_str SELECT 'str' FROM numbers(1);

ALTER TABLE t_full_rewrite_str MODIFY COLUMN s Nullable(String) SETTINGS mutations_sync = 2;
-- Metadata-only: the part still holds non-nullable data.
SELECT type, has(substreams, 's.null') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_full_rewrite_str' AND active AND column = 's';

ALTER TABLE t_full_rewrite_str ADD PROJECTION p1 (SELECT s ORDER BY s);
ALTER TABLE t_full_rewrite_str MATERIALIZE PROJECTION p1 SETTINGS mutations_sync = 2;
-- Every column was re-serialized from the storage-typed pipeline, so the part records
-- Nullable(String) and grows the `.null` substream that type requires.
SELECT type, has(substreams, 's.null') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_full_rewrite_str' AND active AND column = 's';
SELECT s, s.null FROM t_full_rewrite_str;
CHECK TABLE t_full_rewrite_str SETTINGS check_query_single_value_result = 1;

DROP TABLE t_full_rewrite_str;

SELECT '-- String, Compact: same rule';

DROP TABLE IF EXISTS t_full_rewrite_compact;
CREATE TABLE t_full_rewrite_compact (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
         auto_statistics_types = 'tdigest',
         enable_block_number_column = true;

INSERT INTO t_full_rewrite_compact SELECT 'str' FROM numbers(1);
ALTER TABLE t_full_rewrite_compact MODIFY COLUMN s Nullable(String) SETTINGS mutations_sync = 2;
SELECT part_type, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_full_rewrite_compact' AND active AND column = 's';

ALTER TABLE t_full_rewrite_compact ADD PROJECTION p1 (SELECT s ORDER BY s);
ALTER TABLE t_full_rewrite_compact MATERIALIZE PROJECTION p1 SETTINGS mutations_sync = 2;
SELECT type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_full_rewrite_compact' AND active AND column = 's';
SELECT s, s.null FROM t_full_rewrite_compact;

DROP TABLE t_full_rewrite_compact;

SELECT '-- numeric T -> Nullable(T)';

DROP TABLE IF EXISTS t_full_rewrite_num;
CREATE TABLE t_full_rewrite_num (n UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         -- `tdigest` IS supported for UInt64, so statistics have to be disabled outright here.
         auto_statistics_types = '',
         enable_block_number_column = true;

INSERT INTO t_full_rewrite_num SELECT 7 FROM numbers(1);
ALTER TABLE t_full_rewrite_num MODIFY COLUMN n Nullable(UInt64) SETTINGS mutations_sync = 2;
SELECT type, has(substreams, 'n.null') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_full_rewrite_num' AND active AND column = 'n';

ALTER TABLE t_full_rewrite_num ADD PROJECTION p1 (SELECT n ORDER BY n);
ALTER TABLE t_full_rewrite_num MATERIALIZE PROJECTION p1 SETTINGS mutations_sync = 2;
SELECT type, has(substreams, 'n.null') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_full_rewrite_num' AND active AND column = 'n';
SELECT n, n.null FROM t_full_rewrite_num;

DROP TABLE t_full_rewrite_num;

SELECT '-- Variant extension';

DROP TABLE IF EXISTS t_full_rewrite_var;
CREATE TABLE t_full_rewrite_var (k UInt8, v Variant(UInt64, String)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0,
         auto_statistics_types = '',
         enable_block_number_column = true;

INSERT INTO t_full_rewrite_var SELECT 1, 7::UInt64::Variant(UInt64, String) FROM numbers(1);
ALTER TABLE t_full_rewrite_var MODIFY COLUMN v Variant(UInt64, String, Float64) SETTINGS mutations_sync = 2;
SELECT type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_full_rewrite_var' AND active AND column = 'v';

ALTER TABLE t_full_rewrite_var ADD PROJECTION p1 (SELECT k, v ORDER BY k);
ALTER TABLE t_full_rewrite_var MATERIALIZE PROJECTION p1 SETTINGS mutations_sync = 2;
SELECT type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_full_rewrite_var' AND active AND column = 'v';
SELECT k, v FROM t_full_rewrite_var;

DROP TABLE t_full_rewrite_var;

SELECT '-- RENAME of a column whose part type lags';

DROP TABLE IF EXISTS t_full_rewrite_rename;
CREATE TABLE t_full_rewrite_rename (k UInt8, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0,
         auto_statistics_types = 'tdigest',
         enable_block_number_column = true,
         -- The RENAME below must stay a PARTIAL mutation for the next assertion to mean anything.
         -- The randomizer raises `min_bytes_for_full_part_storage`, which makes the part packed and
         -- thus takes the full-rewrite branch via `!isFullPartStorage`.
         min_bytes_for_full_part_storage = 0;

INSERT INTO t_full_rewrite_rename SELECT 1, 'str' FROM numbers(1);
ALTER TABLE t_full_rewrite_rename MODIFY COLUMN s Nullable(String) SETTINGS mutations_sync = 2;
ALTER TABLE t_full_rewrite_rename RENAME COLUMN s TO s2 SETTINGS mutations_sync = 2;
-- A rename alone carries the column's files over, so the stale String survives the rename.
SELECT type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_full_rewrite_rename' AND active AND column = 's2';

ALTER TABLE t_full_rewrite_rename ADD PROJECTION p1 (SELECT k, s2 ORDER BY k);
ALTER TABLE t_full_rewrite_rename MATERIALIZE PROJECTION p1 SETTINGS mutations_sync = 2;
SELECT type, has(substreams, 's2.null') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_full_rewrite_rename' AND active AND column = 's2';
SELECT k, s2, s2.null FROM t_full_rewrite_rename;

DROP TABLE t_full_rewrite_rename;

SELECT '-- must not regress: a partial mutation still keeps the source part type';

DROP TABLE IF EXISTS t_partial_keeps_type;
CREATE TABLE t_partial_keeps_type (k UInt8, s String, other UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = 'tdigest',
         -- Both a packed part (`min_bytes_for_full_part_storage`, raised by the randomizer) and
         -- `enable_block_number_column` would turn the UPDATE below into a full rewrite, which is
         -- the opposite of what this section asserts.
         min_bytes_for_full_part_storage = 0, enable_block_number_column = false;

INSERT INTO t_partial_keeps_type SELECT 1, 'str', 10 FROM numbers(1);
ALTER TABLE t_partial_keeps_type MODIFY COLUMN s Nullable(String) SETTINGS mutations_sync = 2;
-- The metadata-only skip fires: no data is rewritten, so the part keeps String while the storage
-- type is Nullable(String). Breaking this would rewrite whole parts for every T -> Nullable(T).
SELECT 'part', type, has(substreams, 's.null') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_partial_keeps_type' AND active AND column = 's';
SELECT 'storage', type FROM system.columns
WHERE database = currentDatabase() AND table = 't_partial_keeps_type' AND name = 's';

-- A partial mutation of the OTHER column must not disturb `s`: its files are carried over, so its
-- recorded type stays the source part's String.
ALTER TABLE t_partial_keeps_type UPDATE other = 20 WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'part-after-partial', type, has(substreams, 's.null') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_partial_keeps_type' AND active AND column = 's';
SELECT k, s, s.null, other FROM t_partial_keeps_type;

DROP TABLE t_partial_keeps_type;
