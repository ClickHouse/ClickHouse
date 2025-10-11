-- Tags: no-parallel
-- no-parallel: creation of duplicated rules throws exception

DROP TABLE IF EXISTS totals;

CREATE TABLE totals 
(
    date String, hits UInt32, page String
) 
ENGINE = MergeTree() 
PRIMARY KEY (date) 
ORDER BY (date);

INSERT INTO totals 
(
    date, hits, page
) 
VALUES (
    'today', 1337, 'first'
);

SET query_rules = 1;

CREATE RULE rule_1 AS 
(
    SELECT date, sum(hits) FROM stats WHERE page = {name:String} GROUP BY date
) 
REWRITE TO 
(
    SELECT date, hits FROM totals WHERE page = {name:String}
);

SELECT date, sum(hits) FROM stats WHERE page = 'first' GROUP BY date;

ALTER RULE rule_1 AS (
    SELECT date, sum(hits) FROM stats WHERE date = {name2:String} AND page = {name:String} GROUP BY date
)
REWRITE TO 
(
    SELECT date, hits FROM totals WHERE page = {name:String} AND date = {name2:String}
);

SELECT date, sum(hits) FROM stats WHERE date = 'today' AND page = 'first' GROUP BY date;

CREATE RULE rule_2 AS 
(
    SELECT date, hits FROM totals WHERE page = {name:String} AND date = {name2:String}
) 
REWRITE TO 
(
    SELECT date FROM totals WHERE date = {name:String}
);

SELECT date, sum(hits) FROM stats WHERE date = 'today' AND page = 'first' GROUP BY date;

SELECT * FROM system.query_rules FORMAT VERTICAL;

DROP RULE rule_1;

DROP RULE rule_2;

SELECT * FROM system.query_rules FORMAT VERTICAL;

SELECT date, hits FROM totals WHERE page = 'first' AND date = 'today';
