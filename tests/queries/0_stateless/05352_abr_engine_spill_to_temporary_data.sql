-- The result of an attempt that does not fit into `abr_memory_buffer_size` spills to temporary data, which is read back
-- after the attempt succeeds, so the spill files must stay alive until then. A deleted local file stays readable through
-- its open descriptor, so a spill file deleted too early only breaks the query with remote temporary data, as in the
-- CI jobs with `temporary_data_in_cache` or `temporary_data_in_distributed_cache`.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';
SET abr_memory_buffer_size = 0;

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS src_1;

CREATE TABLE src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO src_1 SELECT toDateTime('2026-01-01 00:00:00') + INTERVAL number SECOND, number FROM numbers(100000);

CREATE TABLE abr ENGINE = ABR('src', [1], ('ts'));

-- An aggregated result.
SELECT count(), sum(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00');

-- Many rows, read back in many blocks.
SELECT count(), sum(value) FROM (SELECT value FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') SETTINGS max_block_size = 1000);

DROP TABLE abr;
DROP TABLE src_1;
