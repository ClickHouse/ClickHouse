DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
   uid Int64,
   start Int64,
   end Int64,
   insert_time DateTime
) ENGINE = MergeTree ORDER BY (uid, start);

DROP DICTIONARY IF EXISTS test_dictionary;
CREATE DICTIONARY test_dictionary
(
  start Int64,
  end Int64,
  insert_time DateTime,
  uid Int64
) PRIMARY KEY uid
LAYOUT(RANGE_HASHED())
RANGE(MIN start MAX end)
SOURCE(CLICKHOUSE(TABLE 'test_table' UPDATE_FIELD 'insert_time' UPDATE_LAG 10))
LIFETIME(MIN 1 MAX 2);

INSERT INTO test_table VALUES (1, 0, 100, '2022-12-26 11:38:34'), (1, 101, 200, '2022-12-26 11:38:34'), (2, 0, 999, '2022-12-26 11:38:34'), (2, 1000, 10000, '2022-12-26 11:38:34');

SELECT * FROM test_dictionary;
SELECT dictGet('test_dictionary', 'insert_time', toUInt64(1), 10);

SELECT sleep(3) format Null;
SELECT sleep(3) format Null;

SELECT '--';

SELECT * FROM test_dictionary;
SELECT dictGet('test_dictionary', 'insert_time', toUInt64(1), 10);

DROP DICTIONARY test_dictionary;
DROP TABLE test_table;
