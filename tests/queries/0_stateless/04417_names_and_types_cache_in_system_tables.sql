-- Tests the per-table caches of NamesAndTypesList / ColumnsDescription objects exposed in
-- system.tables. Several parts with the same structure must share a single cache entry, and
-- the *_cache_size columns must report a count while *_cache_bytes report the byte size.

DROP TABLE IF EXISTS t_names_and_types_cache;

CREATE TABLE t_names_and_types_cache (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
SYSTEM STOP MERGES t_names_and_types_cache;

INSERT INTO t_names_and_types_cache VALUES (1, 'x');
INSERT INTO t_names_and_types_cache VALUES (2, 'y');
INSERT INTO t_names_and_types_cache VALUES (3, 'z');

-- Three separate parts, all with the same columns.
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_names_and_types_cache' AND active;

-- The cache holds a single entry shared by all three parts, and the byte counters are positive
-- and larger than the (count-based) size counters.
SELECT
    columns_descriptions_cache_size,
    columns_descriptions_cache_bytes > columns_descriptions_cache_size,
    names_and_types_cache_size,
    names_and_types_cache_bytes > names_and_types_cache_size
FROM system.tables
WHERE database = currentDatabase() AND name = 't_names_and_types_cache';

DROP TABLE t_names_and_types_cache;
