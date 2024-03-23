-- https://github.com/ClickHouse/ClickHouse/issues/50570

DROP TABLE IF EXISTS tnul SYNC;
DROP TABLE IF EXISTS tlc SYNC;

CREATE TABLE tnul (lc Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tnul VALUES (NULL), ('qwe');
SELECT 'pure nullable result:';
SELECT lc FROM tnul WHERE notIn(lc, ('rty', 'uiop'));
DROP TABLE tnul SYNC;


CREATE TABLE tlc (lc LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tlc VALUES (NULL), ('qwe');
SELECT 'wrapping in LC:';
SELECT lc FROM tlc WHERE notIn(lc, ('rty', 'uiop'));
DROP TABLE tlc SYNC;
