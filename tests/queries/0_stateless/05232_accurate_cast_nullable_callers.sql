-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary source tables are not available on parallel-replica workers.

-- Two of the internal users of the accurate cast wrap it in their own null handling, so a text value
-- that a Nullable target cannot represent needs an arm for each of them: a dictionary attribute
-- default (`dictGetOrDefault`) and a set key (`IN` under `transform_null_in = 1`).

DROP DICTIONARY IF EXISTS d_05232;
DROP TABLE IF EXISTS src_05232;
DROP TABLE IF EXISTS keys_05232;

CREATE TABLE src_05232 (k UInt64, a Nullable(UInt32), b UInt32) ENGINE = Memory;
INSERT INTO src_05232 VALUES (1, 10, 100);

CREATE TABLE keys_05232 (k UInt64) ENGINE = Memory;
INSERT INTO keys_05232 VALUES (1), (999);

CREATE TABLE nkeys_05232 (k Nullable(UInt64)) ENGINE = Memory;
INSERT INTO nkeys_05232 VALUES (1), (999), (NULL);

CREATE DICTIONARY d_05232 (k UInt64, a Nullable(UInt32), b UInt32)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src_05232')) LAYOUT(FLAT()) LIFETIME(0);

SELECT 'dictGetOrDefault, constant default';

-- A default the attribute cannot hold is rejected for a Nullable attribute exactly as it already is
-- for the non-Nullable one beside it, whether or not the key is found.
SELECT dictGetOrDefault('d_05232', 'a', toUInt64(999), 'abc');   -- { serverError CANNOT_PARSE_TEXT }
SELECT dictGetOrDefault('d_05232', 'b', toUInt64(999), 'abc');   -- { serverError CANNOT_PARSE_TEXT }
SELECT dictGetOrDefault('d_05232', 'a', toUInt64(999), '99999999999');   -- { serverError CANNOT_PARSE_TEXT }
SELECT dictGetOrDefault('d_05232', 'b', toUInt64(999), '99999999999');   -- { serverError CANNOT_PARSE_TEXT }
SELECT dictGetOrDefault('d_05232', 'a', toUInt64(1), 'abc');   -- { serverError CANNOT_PARSE_TEXT }
SELECT dictGetOrDefault('d_05232', 'b', toUInt64(1), 'abc');   -- { serverError CANNOT_PARSE_TEXT }
SELECT dictGetOrDefault('d_05232', ('a', 'b'), toUInt64(999), ('zz', 9));   -- { serverError CANNOT_PARSE_TEXT }

-- A default the attribute can hold, and a default that is genuinely NULL, are unchanged.
SELECT dictGetOrDefault('d_05232', 'a', toUInt64(999), '42');
SELECT dictGetOrDefault('d_05232', 'a', toUInt64(999), CAST(NULL, 'Nullable(String)'));
SELECT dictGetOrDefault('d_05232', 'a', toUInt64(1), '42');
SELECT dictGetOrDefault('d_05232', ('a', 'b'), toUInt64(999), ('7', 9));

SELECT 'dictGetOrDefault, lazily evaluated default';

-- A lazily evaluated default is only computed for the rows that need it, and only those rows are
-- converted: the others hold the argument type's own default value, which is not the user's value.
SELECT k, dictGetOrDefault('d_05232', 'a', k, toString(toUInt32(k))) FROM keys_05232 ORDER BY k;
SELECT k, dictGetOrDefault('d_05232', 'b', k, toString(toUInt32(k))) FROM keys_05232 ORDER BY k;
SELECT k, dictGetOrDefault('d_05232', 'a', k, if(k = 1, 'abc', '7')) FROM keys_05232 ORDER BY k
    SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT k, dictGetOrDefault('d_05232', 'b', k, if(k = 1, 'abc', '7')) FROM keys_05232 ORDER BY k
    SETTINGS short_circuit_function_evaluation = 'force_enable';

-- A default that is unparsable on a row that does need it fails, and a genuine NULL there is kept.
SELECT k, dictGetOrDefault('d_05232', 'a', k, if(k = 1, '7', 'abc')) FROM keys_05232 ORDER BY k
    SETTINGS short_circuit_function_evaluation = 'force_enable';   -- { serverError CANNOT_PARSE_TEXT }
SELECT k, dictGetOrDefault('d_05232', 'a', k, if(k = 1, '7', CAST(NULL, 'Nullable(String)'))) FROM keys_05232 ORDER BY k
    SETTINGS short_circuit_function_evaluation = 'force_enable';

SELECT 'dictGetOrDefault, NULL key';

-- A NULL key returns NULL from the key's null map, so it never takes the default either.
SELECT k, dictGetOrDefault('d_05232', 'a', k, if(isNull(k), 'abc', '7')) FROM nkeys_05232 ORDER BY k
    SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT k, dictGetOrDefault('d_05232', 'b', k, if(isNull(k), 'abc', '7')) FROM nkeys_05232 ORDER BY k
    SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT k, dictGetOrDefault('d_05232', 'a', k, if(isNull(k), '7', 'abc')) FROM nkeys_05232 ORDER BY k
    SETTINGS short_circuit_function_evaluation = 'force_enable';   -- { serverError CANNOT_PARSE_TEXT }

DROP DICTIONARY d_05232;
DROP TABLE nkeys_05232;
DROP TABLE keys_05232;
DROP TABLE src_05232;

SELECT 'IN with a Nullable set key';

-- `transform_null_in = 1` casts the left column with the accurate cast, so a key the set type cannot
-- represent used to become NULL and then match a NULL in the set.
SELECT materialize('abc') IN (SELECT CAST(NULL, 'Nullable(Int32)')) SETTINGS transform_null_in = 1;   -- { serverError CANNOT_PARSE_TEXT }
SELECT materialize('abc') IN (SELECT CAST(1, 'Nullable(Int32)')) SETTINGS transform_null_in = 1;   -- { serverError CANNOT_PARSE_TEXT }
SELECT materialize('abc') IN (SELECT CAST(1, 'Int32')) SETTINGS transform_null_in = 1;   -- { serverError CANNOT_PARSE_TEXT }
SELECT materialize('999999999999') IN (SELECT CAST(1, 'Nullable(Int32)')) SETTINGS transform_null_in = 1;   -- { serverError CANNOT_PARSE_TEXT }

-- A key the set type can hold, a genuine NULL key, and the default `transform_null_in = 0` path
-- (which casts with `accurateCastOrNull`) are unchanged.
SELECT materialize('1') IN (SELECT CAST(1, 'Nullable(Int32)')) SETTINGS transform_null_in = 1;
SELECT CAST(NULL, 'Nullable(String)') IN (SELECT CAST(NULL, 'Nullable(Int32)')) SETTINGS transform_null_in = 1;
SELECT CAST(NULL, 'Nullable(String)') IN (SELECT CAST(1, 'Nullable(Int32)')) SETTINGS transform_null_in = 1;
SELECT materialize('abc') IN (SELECT CAST(NULL, 'Nullable(Int32)')) SETTINGS transform_null_in = 0;
SELECT materialize('abc') IN (SELECT CAST(1, 'Nullable(Int32)')) SETTINGS transform_null_in = 0;
