-- Coverage for estimateValueSize() branches in StorageGenerateRandom.cpp:
-- String, Array, Map, Nullable, LowCardinality, and Tuple with variable-size
-- nested types force the switch/case paths that are skipped when all columns
-- have a fixed maximum size.

-- String → hits TypeIndex::String branch in estimateValueSize
DROP TABLE IF EXISTS t_gen_string;
CREATE TABLE t_gen_string (s String) ENGINE = GenerateRandom(42, 8, 4);
SELECT length(s) >= 0 FROM t_gen_string LIMIT 1;
DROP TABLE t_gen_string;

-- Array(UInt32) → hits TypeIndex::Array branch
DROP TABLE IF EXISTS t_gen_array;
CREATE TABLE t_gen_array (a Array(UInt32)) ENGINE = GenerateRandom(42, 4, 4);
SELECT length(a) >= 0 FROM t_gen_array LIMIT 1;
DROP TABLE t_gen_array;

-- Map(String, UInt32) → hits TypeIndex::Map branch
DROP TABLE IF EXISTS t_gen_map;
CREATE TABLE t_gen_map (m Map(String, UInt32)) ENGINE = GenerateRandom(42, 4, 4);
SELECT length(m) >= 0 FROM t_gen_map LIMIT 1;
DROP TABLE t_gen_map;

-- Nullable(String) → hits TypeIndex::Nullable branch
DROP TABLE IF EXISTS t_gen_nullable;
CREATE TABLE t_gen_nullable (n Nullable(String)) ENGINE = GenerateRandom(42, 8, 4);
SELECT n IS NULL OR length(n) >= 0 FROM t_gen_nullable LIMIT 1;
DROP TABLE t_gen_nullable;

-- LowCardinality(String) → hits TypeIndex::LowCardinality branch
DROP TABLE IF EXISTS t_gen_lc;
CREATE TABLE t_gen_lc (lc LowCardinality(String)) ENGINE = GenerateRandom(42, 8, 4);
SELECT length(lc) >= 0 FROM t_gen_lc LIMIT 1;
DROP TABLE t_gen_lc;

-- Tuple(String, UInt32) → hits TypeIndex::Tuple branch with variable-size element
DROP TABLE IF EXISTS t_gen_tuple;
CREATE TABLE t_gen_tuple (t Tuple(String, UInt32)) ENGINE = GenerateRandom(42, 8, 4);
SELECT length(t.1) >= 0 FROM t_gen_tuple LIMIT 1;
DROP TABLE t_gen_tuple;
