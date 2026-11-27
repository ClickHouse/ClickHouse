-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

-- Test that empty() and notEmpty() use the primary key for string columns.
-- With optimize_empty_string_comparisons=1 (default), `s = ''` is rewritten
-- to `empty(s)`. This test verifies that empty() and notEmpty() report
-- monotonicity for String arguments so granules can be skipped.

DROP TABLE IF EXISTS t_empty_pk;

CREATE TABLE t_empty_pk (s String)
ENGINE = MergeTree ORDER BY s
SETTINGS index_granularity = 3,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_empty_pk VALUES (''), ('a'), ('b'), ('c'), ('d'), ('e'), ('f'), ('g'), ('h'), ('i');

-- With primary key skipping, searching for '' should read only the first
-- granule (which contains '') and skip all granules that start with a
-- non-empty string.  Searching for non-empty strings should skip the
-- granule that starts with ''.

-- { echo }
EXPLAIN indexes = 1
SELECT * FROM t_empty_pk WHERE empty(s);

EXPLAIN indexes = 1
SELECT * FROM t_empty_pk WHERE s = '';

EXPLAIN indexes = 1
SELECT * FROM t_empty_pk WHERE notEmpty(s);

EXPLAIN indexes = 1
SELECT * FROM t_empty_pk WHERE s != '';
-- { echoOff }

DROP TABLE t_empty_pk;
