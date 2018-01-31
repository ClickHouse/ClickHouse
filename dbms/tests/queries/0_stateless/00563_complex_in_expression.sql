DROP TABLE IF EXISTS test.test;


CREATE TABLE test.test ( dt Date, site_id Int32, site_key String ) ENGINE = MergeTree(dt, (site_id, site_key, dt), 8192);
INSERT INTO test.test (dt,site_id, site_key) VALUES ('2018-1-29', 100, 'key');
SELECT * FROM test.test WHERE toInt32(site_id) IN (100);
SELECT * FROM test.test WHERE toInt32(site_id) IN (100,101);

DROP TABLE IF EXISTS test.test;
