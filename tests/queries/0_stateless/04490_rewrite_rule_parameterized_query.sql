-- Tags: no-parallel
-- no-parallel: creates a globally-visible rewrite rule

-- A query whose matched value is supplied through a query parameter is rewritten correctly:
-- matching runs after the parameter is substituted, so the literal it became is captured by
-- the rule's {name:String} placeholder.

DROP TABLE IF EXISTS rewrite_rule_param_src;
DROP TABLE IF EXISTS rewrite_rule_param_dst;

CREATE TABLE rewrite_rule_param_src (date String, hits UInt32, page String) ENGINE = MergeTree() ORDER BY date;
CREATE TABLE rewrite_rule_param_dst (date String, hits UInt32, page String) ENGINE = MergeTree() ORDER BY date;

INSERT INTO rewrite_rule_param_dst VALUES ('today', 1337, 'first');

CREATE RULE rewrite_rule_param_rule AS
(
    SELECT date, sum(hits) FROM rewrite_rule_param_src WHERE page = {name:String} GROUP BY date
)
REWRITE TO
(
    SELECT date, hits FROM rewrite_rule_param_dst WHERE page = {name:String}
);

SET query_rules = 'rewrite_rule_param_rule';

-- The incoming query parameterizes the matched value with {name:String}; after substitution
-- it becomes the literal 'first', which the rule captures and substitutes into the rewrite.
SET param_name = 'first';
SELECT date, sum(hits) FROM rewrite_rule_param_src WHERE page = {name:String} GROUP BY date;

SET query_rules = '';

DROP RULE rewrite_rule_param_rule;

DROP TABLE rewrite_rule_param_src;
DROP TABLE rewrite_rule_param_dst;
