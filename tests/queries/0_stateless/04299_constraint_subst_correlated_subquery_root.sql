-- Constraint reduction can collapse a filter into a bare correlated subquery
-- (here `b < d` is always false given the constraint, leaving only the subquery).
-- A correlated subquery used as a whole predicate cannot be decorrelated, so the
-- optimization is skipped for such a filter and the query runs as written.

DROP TABLE IF EXISTS t_constraint_corr_root;

CREATE TABLE t_constraint_corr_root
(
    a Nullable(String),
    b String,
    c LowCardinality(String),
    d String,
    CONSTRAINT c1 ASSUME (a <= b) AND (b <= c) AND (c <= d) AND (d <= a)
)
ENGINE = TinyLog;

INSERT INTO t_constraint_corr_root VALUES ('1', '2', '3', '4');

SELECT count() FROM t_constraint_corr_root
WHERE (b < d) OR (SELECT a < c)
SETTINGS enable_analyzer = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_using_constraints = 1;

-- The same query with the optimization disabled must return the same value.
SELECT count() FROM t_constraint_corr_root
WHERE (b < d) OR (SELECT a < c)
SETTINGS enable_analyzer = 1, convert_query_to_cnf = 0, optimize_substitute_columns = 0, optimize_using_constraints = 0;

DROP TABLE t_constraint_corr_root;
