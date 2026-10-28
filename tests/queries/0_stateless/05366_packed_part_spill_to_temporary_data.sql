-- A packed part keeps the data of all its files in memory until the part is finalized.
-- With a small max_bytes_to_buffer_for_packed_part the buffered data of inserts, merges and mutations
-- is moved to the temporary data of the server and read back when the archive is written.

DROP TABLE IF EXISTS t_packed_spill;
DROP TABLE IF EXISTS t_packed_spill_reference;

-- The index granularity is fixed: with a small randomized one, the parts have many granules,
-- and their merges and mutations are too slow in debug builds with the thread fuzzer.
CREATE TABLE t_packed_spill (id UInt64, s String, a Array(UInt64), PROJECTION p_by_s (SELECT id, s ORDER BY s))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi',
    min_bytes_for_full_part_storage = '100G', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    max_bytes_to_buffer_for_packed_part = 65536;

CREATE TABLE t_packed_spill_reference (id UInt64, s String, a Array(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi',
    min_bytes_for_full_part_storage = 0;

INSERT INTO t_packed_spill SELECT number, toString(number * 7919 % 100003), range(number % 10) FROM numbers(200000)
SETTINGS async_insert = 0, log_comment = '05366_insert';
INSERT INTO t_packed_spill_reference SELECT number, toString(number * 7919 % 100003), range(number % 10) FROM numbers(200000);

SELECT 'insert';
SELECT (SELECT sum(cityHash64(*)) FROM t_packed_spill) = (SELECT sum(cityHash64(*)) FROM t_packed_spill_reference);
SELECT count() FROM t_packed_spill WHERE s = '7919' SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT DISTINCT part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_packed_spill' AND active;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalPackedPartWriterWritePart'] > 0, ProfileEvents['ExternalPackedPartWriterUncompressedBytes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05366_insert';

INSERT INTO t_packed_spill SELECT number, toString(number), [number] FROM numbers(200000, 100000) SETTINGS async_insert = 0;
INSERT INTO t_packed_spill_reference SELECT number, toString(number), [number] FROM numbers(200000, 100000);

OPTIMIZE TABLE t_packed_spill FINAL;
OPTIMIZE TABLE t_packed_spill_reference FINAL;

SELECT 'merge';
SELECT (SELECT sum(cityHash64(*)) FROM t_packed_spill) = (SELECT sum(cityHash64(*)) FROM t_packed_spill_reference);
SELECT count() FROM t_packed_spill WHERE s = '7919' SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

ALTER TABLE t_packed_spill UPDATE s = concat(s, '_') WHERE id % 2 = 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_packed_spill_reference UPDATE s = concat(s, '_') WHERE id % 2 = 0 SETTINGS mutations_sync = 2;

SELECT 'mutation';
SELECT (SELECT sum(cityHash64(*)) FROM t_packed_spill) = (SELECT sum(cityHash64(*)) FROM t_packed_spill_reference);
SELECT count() FROM t_packed_spill WHERE s = '7919_' SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT DISTINCT part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_packed_spill' AND active;

SYSTEM FLUSH LOGS part_log;
-- Small background merges may not reach the limit, so check that at least one merge and one mutation spilled.
SELECT event_type, max(ProfileEvents['ExternalPackedPartWriterWritePart'] > 0)
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_packed_spill' AND event_type IN ('MergeParts', 'MutatePart')
GROUP BY event_type
ORDER BY event_type;

DROP TABLE t_packed_spill;
DROP TABLE t_packed_spill_reference;
