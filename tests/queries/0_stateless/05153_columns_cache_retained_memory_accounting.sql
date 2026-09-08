-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- - no-parallel -- `SYSTEM DROP COLUMNS CACHE` is server-wide
-- - no-random-settings, no-random-merge-tree-settings -- the byte counts and the read task layout
--   have to be deterministic
-- - no-replicated-database -- the cache is per server

-- The columns cache is bounded by, and reports, the memory its entries retain - not the logical
-- size of the rows in them.
--
-- A cached column is built by reserving and appending, and `PODArray` rounds a reservation up to
-- a power of two elements and doubles its element storage on growth, so the capacity a finished
-- column holds can exceed the size of its rows by a large factor. The accumulated copy is
-- therefore shrunk to its rows before it is handed to the cache, and what the cache charges,
-- `system.columns_cache` reports and eviction accounts for is the allocated size of the column,
-- so the two agree: an entry costs the memory of its rows and little more.
--
-- A `LowCardinality` column is the case where the difference is not a rounding: the entry keeps
-- the dictionary alive - the cache may be its only holder once the part is gone - while
-- `IColumn::byteSize` deliberately leaves a shared dictionary out. Charging the rows would put
-- the dictionary of every cached range in memory for free, so here the reported bytes have to
-- exceed the dictionary, not just the indexes.
--
-- `columns_cache_max_bytes_to_write_to_cache = 0` lifts the per-query write budget so the whole
-- part is cached in one pass, and `max_threads = 1` with a fixed granularity keeps the part in
-- one read task, so the reported bytes cover exactly the rows the payload is computed over.

DROP TABLE IF EXISTS t_cc_retained;

CREATE TABLE t_cc_retained
(
    id UInt64,
    s String,
    a Array(UInt64),
    lc LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 8192,
    index_granularity_bytes = 0;

INSERT INTO t_cc_retained
SELECT number, repeat('x', 100), range(20), concat(repeat('v', 100), toString(number % 20000))
FROM numbers(60000);

OPTIMIZE TABLE t_cc_retained FINAL;

SYSTEM DROP COLUMNS CACHE;

SELECT sum(id), sum(cityHash64(s)), sum(arraySum(a)), sum(cityHash64(lc)) FROM t_cc_retained
SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    columns_cache_max_bytes_to_write_to_cache = 0,
    columns_cache_max_estimated_compressed_bytes_to_write_to_cache = 0,
    max_threads = 1
FORMAT Null;

-- Every row of every column is cached. Without this the comparisons below would hold the bytes
-- of a part of the rows against the payload of all of them.
SELECT column, sum(rows)
FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cc_retained'
GROUP BY column
ORDER BY column;

WITH
    (SELECT sum(byteSize(id)) FROM t_cc_retained) AS payload_id,
    (SELECT sum(byteSize(s)) FROM t_cc_retained) AS payload_s,
    (SELECT sum(byteSize(a)) FROM t_cc_retained) AS payload_a,
    (SELECT sum(byteSize(v)) FROM (SELECT DISTINCT lc AS v FROM t_cc_retained)) AS payload_lc_dictionary,
    (SELECT sum(bytes) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_retained' AND column = 'id') AS cached_id,
    (SELECT sum(bytes) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_retained' AND column = 's') AS cached_s,
    (SELECT sum(bytes) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_retained' AND column = 'a') AS cached_a,
    (SELECT sum(bytes) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_retained' AND column = 'lc') AS cached_lc,
    (SELECT count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_retained') AS entries
SELECT
    cached_id BETWEEN payload_id AND payload_id + 4096 * entries AS uint64_charges_its_rows_and_little_more,
    cached_s BETWEEN payload_s AND payload_s + 4096 * entries AS string_charges_its_rows_and_little_more,
    cached_a BETWEEN payload_a AND payload_a + 4096 * entries AS array_charges_its_rows_and_little_more,
    cached_lc > payload_lc_dictionary AS low_cardinality_charges_its_dictionary
SETTINGS use_columns_cache = 0;

-- What the cache holds stays within its configured size, measured on that same quantity.
SELECT
    (SELECT sum(bytes) FROM system.columns_cache)
        <= (SELECT value::UInt64 FROM system.server_settings WHERE name = 'columns_cache_size') AS within_configured_size;

DROP TABLE t_cc_retained;
