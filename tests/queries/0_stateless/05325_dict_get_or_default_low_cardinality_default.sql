-- A `LowCardinality` default argument of `dictGetOrDefault` must convert only the values its rows hold,
-- not the default value its dictionary always keeps (an empty `String` cannot be parsed as `UInt32`).

DROP DICTIONARY IF EXISTS d_05325;
DROP TABLE IF EXISTS src_05325;
DROP TABLE IF EXISTS keys_05325;

CREATE TABLE src_05325 (k UInt64, b UInt32) ENGINE = Memory;
INSERT INTO src_05325 VALUES (1, 100);

CREATE TABLE keys_05325 (x UInt64) ENGINE = Memory;
INSERT INTO keys_05325 VALUES (1), (2), (3);

CREATE DICTIONARY d_05325 (k UInt64, b UInt32)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src_05325')) LAYOUT(FLAT()) LIFETIME(0);

SELECT x, dictGetOrDefault('d_05325', 'b', x, toLowCardinality(toString(x + 6))) FROM keys_05325 ORDER BY x;
SELECT x, dictGetOrDefault('d_05325', 'b', 999, toLowCardinality(toString(x + 6))) FROM keys_05325 ORDER BY x;
SELECT x, dictGetOrDefault('d_05325', 'b', CAST(NULL, 'Nullable(UInt64)'), toLowCardinality(toString(x + 6))) FROM keys_05325 ORDER BY x;
SELECT x, dictGetOrDefault('d_05325', 'b', CAST(NULL, 'Nullable(UInt64)'), toLowCardinality(if(x = 1, NULL, '7'))) FROM keys_05325 ORDER BY x;
SELECT x, dictGetUInt32OrDefault('d_05325', 'b', 999, toLowCardinality(toUInt32(x + 6))) FROM keys_05325 ORDER BY x;

-- A default the attribute cannot hold is still rejected.
SELECT x, dictGetOrDefault('d_05325', 'b', 999, toLowCardinality(concat('zz', toString(x)))) FROM keys_05325 ORDER BY x; -- { serverError CANNOT_PARSE_TEXT }

DROP DICTIONARY d_05325;
DROP TABLE keys_05325;
DROP TABLE src_05325;
