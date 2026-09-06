-- Tests for generateRandom() support of JSON, Dynamic, Variant, BFloat16, Time, and Time64 types.
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/92360

SET allow_experimental_json_type = 1;
SET allow_experimental_variant_type = 1;
SET allow_experimental_dynamic_type = 1;

-- BFloat16: fixed-size type, no variable parts.
SELECT count() > 0 FROM generateRandom('x BFloat16', 42, 10, 5) LIMIT 10;

-- Time: stored as Int32.
SELECT count() > 0 FROM generateRandom('x Time', 42, 10, 5) LIMIT 10;

-- Time64: stored as Decimal64.
SELECT count() > 0 FROM generateRandom('x Time64(3)', 42, 10, 5) LIMIT 10;

-- Dynamic: values from a pool of scalar types.
SELECT count() > 0 FROM generateRandom('x Dynamic', 42, 10, 5) LIMIT 10;

-- Variant: random selection among declared variant types per row.
SELECT count() > 0 FROM generateRandom('x Variant(UInt64, String)', 42, 10, 5) LIMIT 10;

-- Variant with a single type.
SELECT count() > 0 FROM generateRandom('x Variant(Int32)', 42, 10, 5) LIMIT 10;

-- JSON without typed paths (only dynamic paths generated).
SELECT count() > 0 FROM generateRandom('x JSON', 42, 10, 5) LIMIT 10;

-- JSON with typed paths: typed paths are filled recursively.
SELECT count() > 0 FROM generateRandom('x JSON(a UInt32, b String)', 42, 10, 5) LIMIT 10;

-- Verify row-level access from a JSON table with typed paths works.
SELECT 1 FROM generateRandom('x JSON(a UInt32)', 42, 10, 5) LIMIT 3;

-- JSON inside an Array.
SELECT count() > 0 FROM generateRandom('x Array(JSON)', 42, 5, 3) LIMIT 5;

-- Dynamic inside an Array.
SELECT count() > 0 FROM generateRandom('x Array(Dynamic)', 42, 5, 3) LIMIT 5;

-- Nullable(Dynamic).
SELECT count() > 0 FROM generateRandom('x Nullable(Dynamic)', 42, 10, 5) LIMIT 10;

-- Using ENGINE = GenerateRandom with JSON type.
DROP TABLE IF EXISTS t_gen_json;
CREATE TABLE t_gen_json (j JSON(a UInt32, b String)) ENGINE = GenerateRandom(42, 10, 5);
SELECT count() > 0 FROM t_gen_json LIMIT 1;
DROP TABLE t_gen_json;

-- Using ENGINE = GenerateRandom with Dynamic type.
DROP TABLE IF EXISTS t_gen_dynamic;
CREATE TABLE t_gen_dynamic (d Dynamic) ENGINE = GenerateRandom(42, 10, 5);
SELECT count() > 0 FROM t_gen_dynamic LIMIT 1;
DROP TABLE t_gen_dynamic;

-- Using ENGINE = GenerateRandom with Variant type.
DROP TABLE IF EXISTS t_gen_variant;
CREATE TABLE t_gen_variant (v Variant(UInt64, Float64, String)) ENGINE = GenerateRandom(42, 10, 5);
SELECT count() > 0 FROM t_gen_variant LIMIT 1;
DROP TABLE t_gen_variant;

-- Verify the argument label bug fix: the 4th positional argument is max_array_length, not max_string_length.
SELECT count() > 0 FROM generateRandom('x Array(UInt32)', 42, 10, 20) LIMIT 1;

-- Verify generate_random_max_json_dynamic_keys setting: 0 means no dynamic paths.
SELECT count() > 0 FROM generateRandom('x JSON', 42, 10, 5) LIMIT 5 SETTINGS generate_random_max_json_dynamic_keys = 0;

-- Verify serialization: render values through FORMAT Null (catches crashes during text serialization).
SELECT x FROM generateRandom('x JSON(a UInt32, b String)', 1, 5, 3) LIMIT 2 FORMAT Null;
SELECT x FROM generateRandom('x Dynamic', 1, 5, 3) LIMIT 5 FORMAT Null;
SELECT x FROM generateRandom('x Variant(UInt64, String)', 1, 5, 3) LIMIT 5 FORMAT Null;
SELECT x FROM generateRandom('x Time', 1, 10, 5) LIMIT 5 FORMAT Null;
SELECT x FROM generateRandom('x Time64(3)', 1, 10, 5) LIMIT 5 FORMAT Null;

-- Verify column types are correct for the new fixed-size types.
SELECT toTypeName(x) FROM generateRandom('x BFloat16', 1, 5, 3) LIMIT 1;
SELECT toTypeName(x) FROM generateRandom('x Time', 1, 5, 3) LIMIT 1;
SELECT toTypeName(x) FROM generateRandom('x Time64(3)', 1, 5, 3) LIMIT 1;
SELECT toTypeName(x) FROM generateRandom('x Dynamic', 1, 5, 3) LIMIT 1;
SELECT toTypeName(x) FROM generateRandom('x Variant(UInt64, String)', 1, 5, 3) LIMIT 1;
