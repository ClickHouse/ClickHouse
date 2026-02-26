DROP DICTIONARY IF EXISTS dict1;
DROP TABLE IF EXISTS dict_src;
DROP TABLE IF EXISTS table1;

CREATE TABLE dict_src (`hash_id` UInt64, `attr1` UInt64, `attr2` UInt64) ENGINE = Memory;
INSERT INTO dict_src VALUES (1, 123, 456), (2, 456, 789);

CREATE TABLE table1 (`hash_id` Nullable(UInt64), `value` UInt64) ENGINE = Memory;
INSERT INTO table1 VALUES (1, 10), (2, 20), (NULL, 30);

CREATE DICTIONARY dict1 (`hash_id` UInt64, `attr1` UInt64, `attr2` UInt64)
PRIMARY KEY hash_id
SOURCE(CLICKHOUSE(TABLE 'dict_src'))
LIFETIME(MIN 0 MAX 0) LAYOUT(HASHED_ARRAY());

SELECT dictGetOrDefault(dict1, 'attr1', hash_id, toUInt64(value)) FROM table1 ORDER BY hash_id;
SELECT dictGetOrDefault(dict1, 'attr1', coalesce(hash_id, 0) :: UInt32, toUInt64(value)) FROM table1 ORDER BY hash_id;
SELECT dictGetOrDefault(dict1, ('attr1', 'attr2'), hash_id, toUInt64(value)) FROM table1 ORDER BY hash_id; -- { serverError UNSUPPORTED_METHOD }
SELECT dictGetOrDefault(dict1, ('attr1', 'attr2'), coalesce(hash_id, 0) :: UInt64, toUInt64(value)) FROM table1 ORDER BY hash_id; -- { serverError TYPE_MISMATCH }

