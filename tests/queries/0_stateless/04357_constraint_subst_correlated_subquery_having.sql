-- Constraint substitution must treat a HAVING clause that is itself a correlated
-- subquery as an opaque boundary: its correlated columns must stay ColumnNodes.

DROP TABLE IF EXISTS t_constraint_corr_having;

CREATE TABLE t_constraint_corr_having
(
    i Int64,
    a Nullable(String),
    b String,
    c LowCardinality(String),
    d String,
    CONSTRAINT c1 ASSUME (a <= b) AND (b <= c) AND (c <= d) AND (d <= a)
)
ENGINE = MergeTree ORDER BY i SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_constraint_corr_having VALUES (1, '1', '2', '3', '4');

SELECT count() FROM t_constraint_corr_having GROUP BY a, b, c, d HAVING (SELECT a < c)
SETTINGS enable_analyzer = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_using_constraints = 1;

-- The same query with the optimization disabled must return the same value.
SELECT count() FROM t_constraint_corr_having GROUP BY a, b, c, d HAVING (SELECT a < c)
SETTINGS enable_analyzer = 1, convert_query_to_cnf = 0, optimize_substitute_columns = 0, optimize_using_constraints = 0;

DROP TABLE t_constraint_corr_having;

-- A correlated subquery in the projection list must also stay opaque to
-- constraint substitution: its correlated columns must remain ColumnNodes.
DROP TABLE IF EXISTS t_constraint_corr_proj;

CREATE TABLE t_constraint_corr_proj
(
    i Int64,
    a Nullable(String),
    b String,
    c LowCardinality(String),
    d String,
    CONSTRAINT c1 ASSUME (a <= b) AND (b <= c) AND (c <= d) AND (d <= a)
)
ENGINE = MergeTree ORDER BY i SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_constraint_corr_proj VALUES (1, '1', '2', '3', '4');

SELECT (SELECT a < c) FROM t_constraint_corr_proj
SETTINGS enable_analyzer = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_using_constraints = 1;

-- The same query with the optimization disabled must return the same value.
SELECT (SELECT a < c) FROM t_constraint_corr_proj
SETTINGS enable_analyzer = 1, convert_query_to_cnf = 0, optimize_substitute_columns = 0, optimize_using_constraints = 0;

DROP TABLE t_constraint_corr_proj;
