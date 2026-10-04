-- Once a mutation or a merge has rewritten a part whose projection was computed from an older type
-- of one of its columns, the new part records the current type while the projection still holds the
-- old values, and nothing on disk distinguishes it from a projection that is up to date. So a
-- mutation must not carry such a projection over, and a merge must not merge it through. This test
-- asserts that, for both kinds of mutation and for a merge, and that an ordinary mutation with no
-- type change keeps its projections.

SET optimize_read_in_order = 0;

SELECT '-- B2. a completed conversion on a part written column per column';
DROP TABLE IF EXISTS t_carry_wide;
CREATE TABLE t_carry_wide
(
    id UInt64,
    b Int32,
    PROJECTION p (SELECT toInt64(b), count() GROUP BY toInt64(b))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_carry_wide SELECT number, toInt32(1073741825 + number * 2) FROM numbers(4);
ALTER TABLE t_carry_wide MODIFY COLUMN b Float32 SETTINGS mutations_sync = 2, alter_sync = 2;
-- the part now records the new type, so its projection cannot be recognised as out of date any more
SELECT 'B2 recorded type', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_carry_wide' AND active AND column = 'b';
SELECT 'B2 projection kept', count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_carry_wide' AND active AND name = 'p';
SELECT 'B2 authoritative', toInt64(b) AS k, count() AS c FROM t_carry_wide GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'B2 with projections', toInt64(b) AS k, count() AS c FROM t_carry_wide GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1;
SELECT 'B2 check';
CHECK TABLE t_carry_wide SETTINGS check_query_single_value_result = 1;
-- this rewrite path reads only the columns it changes, so the projection is left out rather than
-- rebuilt; asking for it explicitly is what puts it back
ALTER TABLE t_carry_wide MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'B2 restored', toInt64(b) AS k, count() AS c FROM t_carry_wide GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_carry_wide;

SELECT '-- B3. a completed conversion on a part written as one file';
DROP TABLE IF EXISTS t_carry_compact;
CREATE TABLE t_carry_compact
(
    id UInt64,
    b Int32,
    PROJECTION p (SELECT toInt64(b), count() GROUP BY toInt64(b))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_full_part_storage = 0, min_bytes_for_wide_part = 1000000000;
INSERT INTO t_carry_compact SELECT number, toInt32(1073741825 + number * 2) FROM numbers(4);
ALTER TABLE t_carry_compact MODIFY COLUMN b Float32 SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'B3 recorded type', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_carry_compact' AND active AND column = 'b';
SELECT 'B3 authoritative', toInt64(b) AS k, count() AS c FROM t_carry_compact GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
-- this rewrite path reads every column, so the projection is rebuilt and stays usable
SELECT 'B3 projection kept', count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_carry_compact' AND active AND name = 'p';
SELECT 'B3 with projections', toInt64(b) AS k, count() AS c FROM t_carry_compact GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'B3 check';
CHECK TABLE t_carry_compact SETTINGS check_query_single_value_result = 1;
DROP TABLE t_carry_compact;

SELECT '-- C2. merging parts cloned from a table where the conversion was still pending';
DROP TABLE IF EXISTS t_merge_src;
DROP TABLE IF EXISTS t_merge_dst;
CREATE TABLE t_merge_src
(
    id UInt64,
    b Int32,
    p UInt8,
    PROJECTION p_e (SELECT toInt64(b), count() GROUP BY toInt64(b))
)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_merge_src SELECT number, toInt32(1073741825 + number * 2), 1 FROM numbers(4);
INSERT INTO t_merge_src SELECT 10 + number, toInt32(1073741841 + number * 2), 1 FROM numbers(4);
SYSTEM STOP MERGES t_merge_src;
ALTER TABLE t_merge_src MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
CREATE TABLE t_merge_dst
(
    id UInt64,
    b Float32,
    p UInt8,
    PROJECTION p_e (SELECT toInt64(b), count() GROUP BY toInt64(b))
)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
ALTER TABLE t_merge_dst ATTACH PARTITION 1 FROM t_merge_src;
OPTIMIZE TABLE t_merge_dst FINAL;
SELECT 'C2 parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_merge_dst' AND active;
SELECT 'C2 authoritative', toInt64(b) AS k, count() AS c FROM t_merge_dst GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'C2 with projections', toInt64(b) AS k, count() AS c FROM t_merge_dst GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'C2 check';
CHECK TABLE t_merge_dst SETTINGS check_query_single_value_result = 1;
DROP TABLE t_merge_src;
DROP TABLE t_merge_dst;

SELECT '-- K. a cancelled conversion, then an unrelated mutation that rewrites the part';
DROP TABLE IF EXISTS t_killed;
CREATE TABLE t_killed
(
    id UInt64,
    b Int32,
    pad UInt64,
    PROJECTION p (SELECT toInt64(b), count() GROUP BY toInt64(b))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_full_part_storage = 0, min_bytes_for_wide_part = 1000000000;
INSERT INTO t_killed SELECT number, toInt32(1073741825 + number * 2), number FROM numbers(4);
SYSTEM STOP MERGES t_killed;
ALTER TABLE t_killed MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_killed' FORMAT Null;
SYSTEM START MERGES t_killed;
-- nothing is left that would ever convert the data, and this mutation rewrites the part anyway
ALTER TABLE t_killed UPDATE pad = pad + 1 WHERE 1 SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'K recorded type', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_killed' AND active AND column = 'b';
SELECT 'K authoritative', toInt64(b) AS k, count() AS c FROM t_killed GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'K with projections', toInt64(b) AS k, count() AS c FROM t_killed GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1;
-- the projection is rebuilt from the current data rather than dropped from the new part
SELECT 'K projection present', count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_killed' AND active AND name = 'p';
SELECT 'K forced', toInt64(b) AS k, count() AS c FROM t_killed GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'K check';
CHECK TABLE t_killed SETTINGS check_query_single_value_result = 1;
DROP TABLE t_killed;

SELECT '-- F2. control: an ordinary mutation with no type change keeps its projections';
DROP TABLE IF EXISTS t_plain_mutation;
CREATE TABLE t_plain_mutation
(
    id UInt64,
    b Int32,
    pad UInt64,
    PROJECTION p (SELECT toInt64(b), count() GROUP BY toInt64(b))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_plain_mutation SELECT number, toInt32(1073741825 + number * 2), number FROM numbers(4);
ALTER TABLE t_plain_mutation UPDATE pad = pad + 1 WHERE 1 SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT 'F2 projection kept', count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_plain_mutation' AND active AND name = 'p';
SELECT 'F2 with projections', toInt64(b) AS k, count() AS c FROM t_plain_mutation GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'F2 check';
CHECK TABLE t_plain_mutation SETTINGS check_query_single_value_result = 1;
DROP TABLE t_plain_mutation;

SELECT '-- L. control: a mutation that only removes files keeps the projection on disk';
DROP TABLE IF EXISTS t_files_only;
CREATE TABLE t_files_only
(
    id UInt64,
    b Int32,
    unrelated String,
    PROJECTION p (SELECT toInt64(b), count() GROUP BY toInt64(b))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_files_only SELECT number, toInt32(1073741825 + number * 2), 'x' FROM numbers(4);
SYSTEM STOP MERGES t_files_only;
ALTER TABLE t_files_only MODIFY COLUMN b Float32 SETTINGS mutations_sync = 0, alter_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_files_only' FORMAT Null;
SYSTEM START MERGES t_files_only;
ALTER TABLE t_files_only DROP COLUMN unrelated SETTINGS mutations_sync = 2, alter_sync = 2;
-- no column data was rewritten, so the part still records the old type and the read falls back
SELECT 'L recorded type', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_files_only' AND active AND column = 'b';
SELECT 'L projection kept', count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_files_only' AND active AND name = 'p';
SELECT 'L authoritative', toInt64(b) AS k, count() AS c FROM t_files_only GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'L with projections', toInt64(b) AS k, count() AS c FROM t_files_only GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1;
SELECT 'L check';
CHECK TABLE t_files_only SETTINGS check_query_single_value_result = 1;
DROP TABLE t_files_only;

SELECT '-- U. a merge of a projection whose key the source part does not record';
DROP TABLE IF EXISTS t_carry_missing_key;
CREATE TABLE t_carry_missing_key (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
-- a materialised `_block_number` / `_block_offset` makes a filter on a column the part does not
-- record match no rows at all, which is wrong before any projection is considered
         enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_carry_missing_key SELECT number FROM numbers(4);
ALTER TABLE t_carry_missing_key ADD COLUMN dt DateTime('UTC') DEFAULT '2026-01-01 05:00:00'
SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_carry_missing_key ADD PROJECTION p (SELECT id, dt ORDER BY dt) SETTINGS alter_sync = 2;
ALTER TABLE t_carry_missing_key MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_carry_missing_key;
ALTER TABLE t_carry_missing_key MODIFY COLUMN dt DateTime('Asia/Tokyo')
SETTINGS mutations_sync = 2, alter_sync = 2;
-- the second part is written after the retype, so it records `dt` at the declared type and carries a
-- projection of its own; the merge then sees a projection part for every source part
INSERT INTO t_carry_missing_key (id) SELECT 100 + number FROM numbers(2);
SYSTEM START MERGES t_carry_missing_key;
OPTIMIZE TABLE t_carry_missing_key FINAL;
SELECT 'U parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_carry_missing_key' AND active;
SELECT 'U authoritative', count(), min(toUInt32(dt)) FROM t_carry_missing_key
WHERE dt = toDateTime('2026-01-01 05:00:00', 'Asia/Tokyo')
SETTINGS optimize_use_projections = 0, use_query_condition_cache = 0, optimize_trivial_count_query = 0;
SELECT 'U with projections', count(), min(toUInt32(dt)) FROM t_carry_missing_key
WHERE dt = toDateTime('2026-01-01 05:00:00', 'Asia/Tokyo')
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1,
         use_query_condition_cache = 0, optimize_trivial_count_query = 0;
SELECT 'U check';
CHECK TABLE t_carry_missing_key SETTINGS check_query_single_value_result = 1;
DROP TABLE t_carry_missing_key;

SELECT '-- X. a merge of an aggregate projection grouped by a column the source part does not record';
DROP TABLE IF EXISTS t_carry_agg_default;
CREATE TABLE t_carry_agg_default (id UInt64, dt DateTime('UTC')) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
-- the two block columns are pinned off for the reason given in arm U
         enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_carry_agg_default SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR
FROM numbers(4);
-- a metadata-only add, so the part records no `h` and the projection freezes the hours the DEFAULT
-- produced while `dt` read as DateTime('UTC')
ALTER TABLE t_carry_agg_default ADD COLUMN h UInt8 DEFAULT toHour(dt) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_carry_agg_default ADD PROJECTION p (SELECT h, count() GROUP BY h) SETTINGS alter_sync = 2;
ALTER TABLE t_carry_agg_default MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_carry_agg_default;
ALTER TABLE t_carry_agg_default MODIFY COLUMN dt DateTime('Asia/Tokyo')
SETTINGS mutations_sync = 2, alter_sync = 2;
-- the second part is written after the retype, so it records `dt` and `h` at the declared types and
-- carries a projection of its own; the merge then sees a projection part for every source part
INSERT INTO t_carry_agg_default (id, dt)
SELECT 100 + number, toDateTime('2026-01-01 00:00:00', 'Asia/Tokyo') + INTERVAL number HOUR FROM numbers(2);
SYSTEM START MERGES t_carry_agg_default;
OPTIMIZE TABLE t_carry_agg_default FINAL;
SELECT 'X parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_carry_agg_default' AND active;
SELECT 'X authoritative', h, count() AS n FROM t_carry_agg_default GROUP BY h ORDER BY h
SETTINGS optimize_use_projections = 0;
SELECT 'X with projections', h, count() AS n FROM t_carry_agg_default GROUP BY h ORDER BY h
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'X check';
CHECK TABLE t_carry_agg_default SETTINGS check_query_single_value_result = 1;
DROP TABLE t_carry_agg_default;
