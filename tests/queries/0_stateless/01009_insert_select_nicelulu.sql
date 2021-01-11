DROP TABLE IF EXISTS test_insert_t1;
DROP TABLE IF EXISTS test_insert_t2;
DROP TABLE IF EXISTS test_insert_t3;

CREATE TABLE test_insert_t1 (`dt` Date, `uid` String, `name` String, `city` String) ENGINE = MergeTree PARTITION BY toYYYYMMDD(dt) ORDER BY name SETTINGS index_granularity = 8192;
CREATE TABLE test_insert_t2 (`dt` Date, `uid` String) ENGINE = MergeTree PARTITION BY toYYYYMMDD(dt) ORDER BY uid SETTINGS index_granularity = 8192;
CREATE TABLE test_insert_t3 (`dt` Date, `uid` String, `name` String, `city` String) ENGINE = MergeTree PARTITION BY toYYYYMMDD(dt) ORDER BY name SETTINGS index_granularity = 8192;

INSERT INTO test_insert_t1 SELECT '2019-09-01',toString(number),toString(rand()),toString(rand()) FROM system.numbers WHERE number > 10 limit 1000000;
INSERT INTO test_insert_t2 SELECT '2019-09-01',toString(number) FROM system.numbers WHERE number >=0 limit 200;
INSERT INTO test_insert_t2 SELECT '2019-09-01',toString(number) FROM system.numbers WHERE number >=100000 limit 200;
INSERT INTO test_insert_t2 SELECT '2019-09-01',toString(number) FROM system.numbers WHERE number >=300000 limit 200;
INSERT INTO test_insert_t2 SELECT '2019-09-01',toString(number) FROM system.numbers WHERE number >=500000 limit 200;
INSERT INTO test_insert_t2 SELECT '2019-09-01',toString(number) FROM system.numbers WHERE number >=700000 limit 200;
INSERT INTO test_insert_t2 SELECT '2019-09-01',toString(number) FROM system.numbers WHERE number >=900000 limit 200;

INSERT INTO test_insert_t3 SELECT '2019-09-01', uid, name, city FROM ( SELECT dt, uid, name, city FROM test_insert_t1 WHERE dt = '2019-09-01') t1 GLOBAL SEMI LEFT JOIN (SELECT uid FROM test_insert_t2 WHERE dt = '2019-09-01') t2 ON t1.uid=t2.uid;

SELECT count(*) FROM test_insert_t3;

DROP TABLE test_insert_t1;
DROP TABLE test_insert_t2;
DROP TABLE test_insert_t3;
