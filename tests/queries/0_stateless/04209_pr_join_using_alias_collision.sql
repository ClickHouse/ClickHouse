-- Regression for MULTIPLE_EXPRESSIONS_FOR_ALIAS on (SELECT *) JOIN (SELECT *) under parallel replicas.
-- https://github.com/ClickHouse/ClickHouse/issues/74324

DROP TABLE IF EXISTS t_pr_join_alias;

CREATE TABLE t_pr_join_alias (id Int8, a Int8, b Int8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pr_join_alias VALUES (1, 2, 3);

SELECT *
FROM (SELECT * FROM t_pr_join_alias) ANY LEFT JOIN (SELECT * FROM t_pr_join_alias) USING id
SETTINGS joined_subquery_requires_alias = 0;

DROP TABLE t_pr_join_alias;
