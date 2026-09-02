DROP TABLE IF EXISTS retention_test;
CREATE TABLE retention_test(date Date, uid Int32)ENGINE = Memory;

INSERT INTO retention_test SELECT '2018-08-06', number FROM numbers(8);
INSERT INTO retention_test SELECT '2018-08-07', number FROM numbers(6);
INSERT INTO retention_test SELECT '2018-08-08', number FROM numbers(7);

SELECT uid, retention(date = '2018-08-06', date = '2018-08-07') AS r FROM retention_test WHERE date IN ('2018-08-06', '2018-08-07') GROUP BY uid ORDER BY uid LIMIT 5;
SELECT '----------';
SELECT uid, retention(1, date = '2018-08-06', date = '2018-08-07') AS r FROM retention_test WHERE date IN ('2018-08-06', '2018-08-07') GROUP BY uid ORDER BY uid LIMIT 5;
SELECT '----------';
SELECT uid, retention(uid % 2 = 0, date = '2018-08-06', date = '2018-08-07') AS r FROM retention_test WHERE date IN ('2018-08-06', '2018-08-07') GROUP BY uid ORDER BY uid LIMIT 5;

DROP TABLE IF EXISTS retention_test;
