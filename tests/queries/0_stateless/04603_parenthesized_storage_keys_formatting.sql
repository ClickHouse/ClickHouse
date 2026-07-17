-- Parentheses around key clauses and other expressions in a table definition are redundant
-- and must not be preserved: stored table metadata relies on the canonical form without them.
-- https://github.com/ClickHouse/ClickHouse/pull/92340 started to preserve them, which broke
-- comparisons of the metadata with tables created by older versions.

SELECT formatQuerySingleLine('CREATE TABLE t (a Int, b Int) ENGINE = MergeTree PARTITION BY (a) PRIMARY KEY (a) ORDER BY (a) SAMPLE BY (a)');
SELECT formatQuerySingleLine('CREATE TABLE t (a Int, b Int) ENGINE = MergeTree PARTITION BY (a, b) ORDER BY (a, b)');
SELECT formatQuerySingleLine('CREATE TABLE t (a Int, b Int) ENGINE = MergeTree ORDER BY ((a), (b))');
SELECT formatQuerySingleLine('CREATE TABLE t (a Int, b Int) ENGINE = MergeTree ORDER BY (a) DESC');
SELECT formatQuerySingleLine('CREATE TABLE t (a Int, b Int, PRIMARY KEY (a)) ENGINE = MergeTree ORDER BY a');
SELECT formatQuerySingleLine('CREATE TABLE t (a Int, d DateTime) ENGINE = MergeTree ORDER BY a TTL (d + INTERVAL 1 DAY) WHERE (a > 0)');
SELECT formatQuerySingleLine('CREATE TABLE t (a Int, d DateTime, v Int) ENGINE = MergeTree ORDER BY (a, d) TTL (d + INTERVAL 1 DAY) GROUP BY (a) SET v = (max(v))');
SELECT formatQuerySingleLine('CREATE TABLE t (a Int, d DateTime, i Int DEFAULT (a + 1), m Int MATERIALIZED (a * 2), c Int TTL (d + INTERVAL 1 DAY)) ENGINE = MergeTree ORDER BY a');
SELECT formatQuerySingleLine('CREATE TABLE t (a Int, INDEX ix (a) TYPE minmax, CONSTRAINT cc CHECK (a > 0)) ENGINE = MergeTree ORDER BY a');
SELECT formatQuerySingleLine('CREATE TABLE t (a Int, b Int, PROJECTION p (SELECT a, b GROUP BY (a)), PROJECTION p2 (SELECT a ORDER BY (a))) ENGINE = MergeTree ORDER BY a');
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY ORDER BY (a)');
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY SAMPLE BY (a)');
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY TTL (d + INTERVAL 1 DAY)');

-- Aliased expressions keep the parentheses: without them the alias could change the meaning
-- of the surrounding clause.
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree PARTITION BY (c0 AS p) PRIMARY KEY (c0 AS k) ORDER BY c0 SAMPLE BY (c0 AS s)');

-- The formatting must be stable.
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (a Int, b Int) ENGINE = MergeTree PARTITION BY (a) PRIMARY KEY (a) ORDER BY (a) SAMPLE BY (a)'));
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE TABLE t (c0 Int64) ENGINE = MergeTree PARTITION BY (c0 AS p) PRIMARY KEY (c0 AS k) ORDER BY c0 SAMPLE BY (c0 AS s)'));

-- SHOW CREATE TABLE must render the canonical form as well.
DROP TABLE IF EXISTS t_04603;
CREATE TABLE t_04603 (a Int, d DateTime, i Int DEFAULT (a + 1), INDEX ix (a) TYPE minmax, CONSTRAINT cc CHECK (a > 0)) ENGINE = MergeTree PARTITION BY (a) PRIMARY KEY (a) ORDER BY (a) TTL (d + INTERVAL 1 DAY);
SHOW CREATE TABLE t_04603;
DROP TABLE t_04603;
