SET enable_analyzer = 1;

-- { echoOn }
SET limit = 0;

SELECT * FROM numbers(10);
SELECT * FROM numbers(10) SETTINGS limit=5, offset=2;
SELECT count(*) FROM (SELECT * FROM numbers(10));
SELECT count(*) FROM (SELECT * FROM numbers(10) SETTINGS limit=5);
SELECT count(*) FROM (SELECT * FROM numbers(10)) SETTINGS limit=5;
SELECT count(*) FROM view(SELECT * FROM numbers(10));
SELECT count(*) FROM view(SELECT * FROM numbers(10) SETTINGS limit=5);
SELECT count(*) FROM view(SELECT * FROM numbers(10)) SETTINGS limit=5;

SET limit = 3;
SELECT * FROM numbers(10);
SELECT * FROM numbers(10) SETTINGS limit=5, offset=2;
SELECT count(*) FROM (SELECT * FROM numbers(10));
SELECT count(*) FROM (SELECT * FROM numbers(10) SETTINGS limit=5);
SELECT count(*) FROM (SELECT * FROM numbers(10)) SETTINGS limit=5;
SELECT count(*) FROM view(SELECT * FROM numbers(10));
SELECT count(*) FROM view(SELECT * FROM numbers(10) SETTINGS limit=5);
SELECT count(*) FROM view(SELECT * FROM numbers(10)) SETTINGS limit=5;

SET limit = 4;
SET offset = 1;
SELECT * FROM numbers(10);
SELECT * FROM numbers(10) LIMIT 3 OFFSET 2;
SELECT * FROM numbers(10) LIMIT 5 OFFSET 2;
-- { echoOff }
