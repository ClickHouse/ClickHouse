SET allow_experimental_analyzer = 1;

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
-- { echoOff }
