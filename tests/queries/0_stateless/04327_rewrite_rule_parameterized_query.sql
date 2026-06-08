-- Tags: no-parallel
-- no-parallel: creates a globally-visible rewrite rule

-- Regression test: when both the rule source and the incoming query are parameterized with
-- the same {p:Type} placeholder, matching short-circuited on equal subtree hashes and never
-- populated the capture map, causing applyRule to throw REWRITE_RULE_UNKNOWN_QUERY_PARAMETER.

DROP TABLE IF EXISTS rewrite_rule_param_src;
DROP TABLE IF EXISTS rewrite_rule_param_dst;

CREATE TABLE rewrite_rule_param_src (date String, hits UInt32, page String) ENGINE = MergeTree() ORDER BY date;
CREATE TABLE rewrite_rule_param_dst (date String, hits UInt32, page String) ENGINE = MergeTree() ORDER BY date;

INSERT INTO rewrite_rule_param_dst VALUES ('today', 1337, 'first');

SET query_rules = 1;

CREATE RULE rewrite_rule_param_rule AS
(
    SELECT date, sum(hits) FROM rewrite_rule_param_src WHERE page = {name:String} GROUP BY date
)
REWRITE TO
(
    SELECT date, hits FROM rewrite_rule_param_dst WHERE page = {name:String}
);

-- Incoming query is itself parameterized with the same placeholder {name:String}.
-- Before the fix this threw REWRITE_RULE_UNKNOWN_QUERY_PARAMETER.
SET param_name = 'first';
SELECT date, sum(hits) FROM rewrite_rule_param_src WHERE page = {name:String} GROUP BY date;

DROP RULE rewrite_rule_param_rule;

DROP TABLE rewrite_rule_param_src;
DROP TABLE rewrite_rule_param_dst;
