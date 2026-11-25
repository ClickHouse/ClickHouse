-- A constant NULL key of `dictGetOrDefault` takes NULL from the key, never the default, so a lazily
-- evaluated default the attribute cannot hold must not be converted for it.

SET short_circuit_function_evaluation = 'force_enable';

DROP DICTIONARY IF EXISTS d_05233;
DROP TABLE IF EXISTS src_05233;
DROP TABLE IF EXISTS keys_05233;

CREATE TABLE src_05233 (k UInt64, a Nullable(UInt32), b UInt32) ENGINE = Memory;
INSERT INTO src_05233 VALUES (1, 10, 100);

CREATE TABLE keys_05233 (x UInt64) ENGINE = Memory;
INSERT INTO keys_05233 VALUES (1), (2);

CREATE DICTIONARY d_05233 (k UInt64, a Nullable(UInt32), b UInt32)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src_05233')) LAYOUT(FLAT()) LIFETIME(0);

SELECT x, dictGetOrDefault('d_05233', 'a', CAST(NULL, 'Nullable(UInt64)'), concat('zz', toString(x)))
FROM keys_05233 ORDER BY x;

SELECT x, dictGetOrDefault('d_05233', 'b', CAST(NULL, 'Nullable(UInt64)'), if(x = 1, NULL, '7'))
FROM keys_05233 ORDER BY x;

-- A constant key that is merely missing from the dictionary does take the default, and converts it.
SELECT x, dictGetOrDefault('d_05233', 'a', CAST(999, 'Nullable(UInt64)'), concat('zz', toString(x)))
FROM keys_05233 ORDER BY x;   -- { serverError CANNOT_PARSE_TEXT }

SELECT x, dictGetOrDefault('d_05233', 'a', CAST(999, 'Nullable(UInt64)'), toString(x + 7))
FROM keys_05233 ORDER BY x;

DROP DICTIONARY d_05233;
DROP TABLE keys_05233;
DROP TABLE src_05233;
