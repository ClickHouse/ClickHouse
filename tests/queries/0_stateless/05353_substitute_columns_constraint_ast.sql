-- Covers SubstituteColumnOptimizer: with optimize_substitute_columns the AST optimizer replaces a
-- column by an equivalent expression taken from a satisfied equality CONSTRAINT, picking whichever
-- side of the equality is cheaper to read -- see optimize_substitute_columns in
-- docs/en/operations/settings/settings.md. That optimizer runs in the pre-analyzer AST pass, which
-- is observable through EXPLAIN AST optimize = 1 (docs/en/sql-reference/statements/explain.md).

SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_substitute_columns = 1;
-- optimize_append_index is randomized by clickhouse-test and appends extra index conditions to
-- WHERE, which would change the AST node counts asserted below.
SET optimize_append_index = 0;

DROP TABLE IF EXISTS t_substitute;
DROP TABLE IF EXISTS t_no_constraint;
DROP TABLE IF EXISTS t_empty;

-- ORDER BY a keeps the choice independent of the real column sizes: a is a primary key column and
-- is therefore free to read, so cityHash64(a) always wins over reading the stored column b.
CREATE TABLE t_substitute (i Int64, a String, b UInt64,
    CONSTRAINT c1 ASSUME b = cityHash64(a),
    CONSTRAINT c2 ASSUME i > 0)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_substitute SELECT number + 1, toString(number), cityHash64(toString(number)) FROM numbers(20);

CREATE TABLE t_no_constraint (i Int64, b UInt64) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_no_constraint SELECT number, number FROM numbers(5);

-- Same constraint, but left empty on purpose: a table without parts reports no column sizes.
CREATE TABLE t_empty (i Int64, a String, b UInt64, CONSTRAINT c1 ASSUME b = cityHash64(a))
ENGINE = MergeTree ORDER BY a;

-- Each check counts, in the optimized AST, how often the substituted expression cityHash64(a)
-- appears and how often the replaced column b still appears. A substituted query has no b left.

-- SELECT list and WHERE are both rewritten. In the SELECT list the substituted expression must also
-- keep the replaced name as its alias, so that it is still exposed downstream as column b -> 2 0 1
SELECT countIf(explain LIKE '%Function cityHash64%'), countIf(explain LIKE '%Identifier b%'),
       countIf(explain LIKE '%(alias b)%')
FROM (EXPLAIN AST optimize = 1 SELECT i, b FROM t_substitute WHERE b = 123);

-- PREWHERE is rewritten too -> 2 0
SELECT countIf(explain LIKE '%Function cityHash64%'), countIf(explain LIKE '%Identifier b%')
FROM (EXPLAIN AST optimize = 1 SELECT i FROM t_substitute PREWHERE b = 123 WHERE i > 1);

-- HAVING is rewritten too -> 1 0
SELECT countIf(explain LIKE '%Function cityHash64%'), countIf(explain LIKE '%Identifier b%')
FROM (EXPLAIN AST optimize = 1 SELECT a FROM t_substitute WHERE i > 1 GROUP BY a HAVING b = 123);

-- No constraints at all: nothing is equivalent to b, so b stays -> 0 2
SELECT countIf(explain LIKE '%Function cityHash64%'), countIf(explain LIKE '%Identifier b%')
FROM (EXPLAIN AST optimize = 1 SELECT b FROM t_no_constraint WHERE b = 1);

-- Constraint present but no column sizes to compare, so b stays -> 0 2
SELECT countIf(explain LIKE '%Function cityHash64%'), countIf(explain LIKE '%Identifier b%')
FROM (EXPLAIN AST optimize = 1 SELECT b FROM t_empty WHERE b = 2);

-- Control: with the setting off the same query keeps b -> 0 2
SELECT countIf(explain LIKE '%Function cityHash64%'), countIf(explain LIKE '%Identifier b%')
FROM (EXPLAIN AST optimize = 1 SELECT i, b FROM t_substitute WHERE b = 123)
SETTINGS optimize_substitute_columns = 0;

-- The alias is what makes the substitution transparent to the rest of the query. The outer SELECT
-- resolves the identifier b against the column names of the already rewritten subquery, so without
-- the alias this query would fail with UNKNOWN_IDENTIFIER instead of printing a row -> 1 1
SELECT countIf(explain LIKE '%Function cityHash64 (alias b)%'), countIf(explain LIKE '%Identifier b%')
FROM (EXPLAIN AST optimize = 1 SELECT b FROM (SELECT b FROM t_substitute WHERE a = '7'));

-- The rewrite is a read-cost choice only, so the answers themselves do not change.
SELECT b = cityHash64('7') FROM (SELECT b FROM t_substitute WHERE a = '7');
SELECT count() FROM t_substitute WHERE b = cityHash64(a);
SELECT count() FROM t_substitute WHERE b = 123;
SELECT i, b = cityHash64(a) FROM t_substitute WHERE a = '7';

DROP TABLE t_substitute;
DROP TABLE t_no_constraint;
DROP TABLE t_empty;
