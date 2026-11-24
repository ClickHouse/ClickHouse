-- A mutation that rebuilds a projection writes it into temporary projection parts that have their own transaction,
-- so they do not get the temporary data of the parent part and must move their buffered data out of memory by themselves.
-- The same mutation runs on a table without the projection: the main parts of both tables are the same, so they move
-- the same data, and anything moved in addition is the projection. The data of the projection fits into one block,
-- so it is written as a single temporary projection part that becomes the projection without a merge of projection parts.

DROP TABLE IF EXISTS t_packed_spill_with_projection;
DROP TABLE IF EXISTS t_packed_spill_without_projection;

-- The index granularity is fixed: with a small randomized one, the parts have many granules,
-- and their merges and mutations are too slow in debug builds with the thread fuzzer.
CREATE TABLE t_packed_spill_with_projection (id UInt64, s String, PROJECTION p_by_s (SELECT id, s ORDER BY s))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi',
    min_bytes_for_full_part_storage = '100G', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    max_bytes_to_buffer_for_packed_part = 65536;

CREATE TABLE t_packed_spill_without_projection (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi',
    min_bytes_for_full_part_storage = '100G', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    max_bytes_to_buffer_for_packed_part = 65536;

INSERT INTO t_packed_spill_with_projection SELECT number, toString(number * 7919 % 100003) FROM numbers(200000) SETTINGS async_insert = 0;
INSERT INTO t_packed_spill_without_projection SELECT number, toString(number * 7919 % 100003) FROM numbers(200000) SETTINGS async_insert = 0;

-- An insert can produce several parts, so merge them to get the same single part in both tables.
OPTIMIZE TABLE t_packed_spill_with_projection FINAL;
OPTIMIZE TABLE t_packed_spill_without_projection FINAL;

SELECT table, count(), any(part_storage_type) FROM system.parts
WHERE database = currentDatabase() AND table LIKE 't_packed_spill_%' AND active
GROUP BY table ORDER BY table;

ALTER TABLE t_packed_spill_with_projection UPDATE s = concat(s, '_') WHERE id % 2 = 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_packed_spill_without_projection UPDATE s = concat(s, '_') WHERE id % 2 = 0 SETTINGS mutations_sync = 2;

SELECT count() FROM t_packed_spill_with_projection WHERE s = '7919_' SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT sum(cityHash64(id, s)) FROM t_packed_spill_with_projection WHERE s >= '5' SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT sum(cityHash64(id, s)) FROM t_packed_spill_without_projection WHERE s >= '5';

SYSTEM FLUSH LOGS part_log;
WITH
    (
        SELECT sum(ProfileEvents['ExternalPackedPartWriterUncompressedBytes']) FROM system.part_log
        WHERE database = currentDatabase() AND table = 't_packed_spill_with_projection' AND event_type = 'MutatePart'
    ) AS with_projection,
    (
        SELECT sum(ProfileEvents['ExternalPackedPartWriterUncompressedBytes']) FROM system.part_log
        WHERE database = currentDatabase() AND table = 't_packed_spill_without_projection' AND event_type = 'MutatePart'
    ) AS without_projection
SELECT without_projection > 0, with_projection > without_projection;

DROP TABLE t_packed_spill_with_projection;
DROP TABLE t_packed_spill_without_projection;
