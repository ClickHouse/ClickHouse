-- https://github.com/ClickHouse/ClickHouse/issues/96588
-- CHECK TABLE on a Tuple with a Dynamic element and a sparse-serialized element
-- used to fail with "Unexpected size of tuple element" because deserializeOffsets
-- in SerializationSparse treated limit=0 as "read everything" instead of "read nothing".

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Tuple(c1 Dynamic, c2 Tuple(c3 Int))) ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1, ratio_of_defaults_for_sparse_serialization = 0.9;
-- The positional tuple literal `(number, )` must be inserted by position, not by element name,
-- so disable `enable_named_columns_in_function_tuple` for this INSERT.
INSERT INTO TABLE t0 (c0) SELECT (1, (number, ), ) FROM numbers(1) SETTINGS enable_named_columns_in_function_tuple = 0;
CHECK TABLE t0;

DROP TABLE t0;
