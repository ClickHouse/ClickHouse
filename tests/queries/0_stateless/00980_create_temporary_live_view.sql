SET allow_experimental_live_view = 1;

DROP TABLE IF EXISTS test.lv;
DROP TABLE IF EXISTS test.mt;

SELECT name, value from system.settings WHERE name = 'temporary_live_view_timeout';
SELECT name, value from system.settings WHERE name = 'live_view_heartbeat_interval';

CREATE TABLE test.mt (a Int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW test.lv WITH TIMEOUT 1 AS SELECT sum(a) FROM test.mt;

SHOW TABLES FROM test LIKE 'lv';
SELECT sleep(2);
SHOW TABLES FROM test LIKE 'lv';

DROP TABLE test.mt;
