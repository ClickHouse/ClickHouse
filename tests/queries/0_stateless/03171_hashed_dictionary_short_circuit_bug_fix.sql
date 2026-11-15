CREATE TABLE x ( hash_id UInt64, user_result Decimal(3, 2) ) ENGINE = Memory();

CREATE TABLE y ( hash_id UInt64, user_result  DECIMAL(18, 6) ) ENGINE = Memory();

INSERT INTO x values (100, 1), (200, 2);
INSERT INTO y values (100, 1), (300, 3), (200, 2);

CREATE DICTIONARY d1 (hash_id UInt64, user_result Decimal(3, 2) )
PRIMARY KEY hash_id
SOURCE(CLICKHOUSE(TABLE 'x'))
LIFETIME(0)
LAYOUT(HASHED());

SELECT hash_id,
  dictGetOrDefault(d1, 'user_result', toUInt64(hash_id), toFloat64(user_result)),
  dictGet(d1, 'user_result', toUInt64(hash_id))
FROM y;

CREATE DICTIONARY d2 (hash_id UInt64, user_result Decimal(3, 2) )
PRIMARY KEY hash_id
SOURCE(CLICKHOUSE(TABLE 'x'))
LIFETIME(0)
LAYOUT(HASHED_ARRAY());

SELECT hash_id,
  dictGetOrDefault(d2, 'user_result', toUInt64(hash_id), toFloat64(user_result)),
  dictGet(d2, 'user_result', toUInt64(hash_id))
FROM y;
