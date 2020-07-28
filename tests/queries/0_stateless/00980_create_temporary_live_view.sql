SET allow_experimental_live_view = 1;

DROP TABLE IF EXISTS lv;
DROP TABLE IF EXISTS mt;

SELECT name, value from system.settings WHERE name = 'temporary_live_view_timeout';
SELECT name, value from system.settings WHERE name = 'live_view_heartbeat_interval';

CREATE TABLE mt (a Int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW lv WITH TIMEOUT 1 AS SELECT sum(a) FROM mt;

SHOW TABLES WHERE database=currentDatabase() and name LIKE 'lv';
SELECT sleep(2);
SHOW TABLES WHERE database=currentDatabase() and name LIKE 'lv';

DROP TABLE mt;
