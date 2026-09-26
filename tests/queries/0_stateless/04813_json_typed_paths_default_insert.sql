-- Default filling of JSON typed paths during insert/parsing.
-- Covers both the trivial fast path and the non-trivial path (Enum, whose
-- default is the first enum value, not zero), plus Nullable typed paths.

-- Reading a subcolumn of an aliased expression (j.e) requires the analyzer.
SET enable_analyzer = 1;

-- Trivial defaults only (Int64 typed paths): fast path in ObjectJSONNode.
SELECT '{}'::JSON(a Int64, b Int64) AS j;

-- Non-trivial default (Enum typed path): the enum default must be the first
-- enum value ('x'), not 0.
SELECT '{}'::JSON(a Int64, e Enum8('x' = 1, 'y' = 2)) AS j, j.e;

-- Object present but some typed paths absent: the absent ones get defaults.
SELECT '{"a": 10}'::JSON(a Int64, b Int64, e Enum8('x' = 1, 'y' = 2)) AS j;

-- Nested JSON value is null with null_as_default: default-fill branch.
SELECT '{"nested": null}'::JSON(nested JSON(a Int64, e Enum8('x' = 1, 'y' = 2))) AS j;

-- Nullable typed paths default to NULL.
SELECT '{}'::JSON(n Nullable(Enum8('p' = 5, 'q' = 6)), m Nullable(Int64)) AS j, j.n, j.m;

-- Insert deduplication hashes the nested bytes of null rows too, so an omitted
-- path and an explicit null must store the same value hidden under NULL.
SELECT toInt8(assumeNotNull('{}'::JSON(n Nullable(Enum8('p' = 5, 'q' = 6))).n)) AS omitted,
       toInt8(assumeNotNull('{"n": null}'::JSON(n Nullable(Enum8('p' = 5, 'q' = 6))).n)) AS explicit_null;

-- Default filling during INSERT into MergeTree, mixing default-filled rows and
-- explicitly provided rows.
DROP TABLE IF EXISTS t_json_typed_paths_default;
CREATE TABLE t_json_typed_paths_default (id UInt64, data JSON(a Int64, e Enum8('x' = 1, 'y' = 2)))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json_typed_paths_default SELECT number, '{}' FROM numbers(3);
INSERT INTO t_json_typed_paths_default VALUES (10, '{"a": 5, "e": "y"}');

SELECT id, data, data.a, data.e FROM t_json_typed_paths_default ORDER BY id;

DROP TABLE t_json_typed_paths_default;
