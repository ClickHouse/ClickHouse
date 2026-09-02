DROP TABLE IF EXISTS new_table_test;
DROP TABLE IF EXISTS check_table_test;

CREATE TABLE new_table_test(name String) ENGINE = MergeTree ORDER BY name;
INSERT INTO new_table_test VALUES ('test');
CREATE TABLE check_table_test(value1 UInt64, value2 UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO check_table_test (value1) SELECT value FROM system.events WHERE event = 'Merge';
OPTIMIZE TABLE new_table_test FINAL;
INSERT INTO check_table_test (value2) SELECT value FROM system.events WHERE event = 'Merge';
SELECT count() FROM check_table_test WHERE value2 > value1;


DROP TABLE new_table_test;
DROP TABLE check_table_test;
