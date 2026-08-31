-- `max_memory_ratio_to_activate_adaptive_write_buffer` lets a wide part switch to adaptive write
-- buffers because of how much memory the preallocated buffers would take, not only because the table
-- has at least `min_columns_to_activate_adaptive_write_buffer` columns. A tiny ratio forces the
-- adaptive path on for every stream of this narrow table, so the round-trip below covers it for the
-- plain, Array, Nullable and LowCardinality substreams.

DROP TABLE IF EXISTS t_adaptive_write_buffer;

CREATE TABLE t_adaptive_write_buffer
(
    a UInt64,
    s String,
    arr Array(UInt32),
    n Nullable(String),
    lc LowCardinality(String)
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0,
    min_columns_to_activate_adaptive_write_buffer = 0,
    max_memory_ratio_to_activate_adaptive_write_buffer = 0.000000001,
    adaptive_write_buffer_initial_size = 4096,
    max_compress_block_size = 1048576;

INSERT INTO t_adaptive_write_buffer
SELECT
    number,
    repeat('x', number % 100),
    range(number % 7),
    if(number % 3 = 0, NULL, toString(number)),
    toString(number % 10)
FROM numbers(200000);

SELECT count(), sum(a), sum(length(s)), sum(length(arr)), countIf(n IS NULL), uniqExact(lc) FROM t_adaptive_write_buffer;
SELECT count() FROM t_adaptive_write_buffer WHERE s = repeat('x', 42);
SELECT a, s, arr, n, lc FROM t_adaptive_write_buffer WHERE a IN (0, 199999) ORDER BY a;

-- The parts must be wide, otherwise the per-stream write buffers this setting is about do not exist.
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_adaptive_write_buffer' AND active;

DROP TABLE t_adaptive_write_buffer;
