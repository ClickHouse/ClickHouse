DROP TABLE IF EXISTS pk_func;

CREATE TABLE pk_func (`d` DateTime, `ui` UInt32 ) ENGINE = MergeTree ORDER BY toDate(d);
INSERT INTO pk_func SELECT '2020-05-05 01:00:00', number FROM numbers(1000);
SELECT 1, * FROM pk_func ORDER BY toDate(d) ASC, ui ASC LIMIT 3;

DROP TABLE IF EXISTS pk_func;
