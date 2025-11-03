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
    SELECT date, hits FROM totals WHERE page = {name:String}
) 
REJECT WITH 'Was rejected by rule_1';

SELECT date, hits FROM totals WHERE page = 'first'; -- { serverError REWRITE_RULE_REJECTION }

DROP RULE rule_1;
