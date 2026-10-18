-- A clone of a packed part that rewrites its archive buffers the whole part, so it moves the data
-- to the temporary data of the server once it exceeds max_bytes_to_buffer_for_packed_part.

DROP TABLE IF EXISTS t_packed_clone_spill;
DROP TABLE IF EXISTS t_packed_clone_spill_dst;

CREATE TABLE t_packed_clone_spill (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi',
    min_bytes_for_full_part_storage = '100G', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    max_bytes_to_buffer_for_packed_part = 65536, always_use_copy_instead_of_hardlinks = 1;

CREATE TABLE t_packed_clone_spill_dst (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi',
    min_bytes_for_full_part_storage = '100G', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    max_bytes_to_buffer_for_packed_part = 65536, always_use_copy_instead_of_hardlinks = 0;

INSERT INTO t_packed_clone_spill SELECT number, toString(sipHash64(number)) FROM numbers(100000) SETTINGS async_insert = 0;

-- The mutation does not change any row, so the part is copied instead of being mutated.
ALTER TABLE t_packed_clone_spill UPDATE s = 'x' WHERE id >= 100000 SETTINGS mutations_sync = 2;

SELECT 'mutation';
SELECT count(), sum(cityHash64(id, s)) = (SELECT sum(cityHash64(number, toString(sipHash64(number)))) FROM numbers(100000))
FROM t_packed_clone_spill;
SELECT name, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_packed_clone_spill' AND active;

SYSTEM FLUSH LOGS part_log;
SELECT ProfileEvents['ExternalPackedPartWriterWritePart'] > 0
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_packed_clone_spill' AND event_type = 'MutatePart';

-- The parts attached from another table do not keep the metadata version of the source, so their archives are rewritten
-- even without always_use_copy_instead_of_hardlinks.
ALTER TABLE t_packed_clone_spill_dst REPLACE PARTITION ID 'all' FROM t_packed_clone_spill SETTINGS log_comment = '05370_replace';

SELECT 'replace partition';
SELECT count(), sum(cityHash64(id, s)) = (SELECT sum(cityHash64(number, toString(sipHash64(number)))) FROM numbers(100000))
FROM t_packed_clone_spill_dst;
SELECT part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_packed_clone_spill_dst' AND active;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalPackedPartWriterWritePart'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05370_replace';

DROP TABLE t_packed_clone_spill;
DROP TABLE t_packed_clone_spill_dst;
