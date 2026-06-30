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

CREATE RULE rule_1 AS
(
    SELECT date, hits FROM totals WHERE page = {name:String}
)
REJECT WITH 'Was rejected by rule_1';

SET query_rules = 'rule_1';

SELECT date, hits FROM totals WHERE page = 'first'; -- { serverError REWRITE_RULE_REJECTION }

SET query_rules = '';

DROP RULE rule_1;
