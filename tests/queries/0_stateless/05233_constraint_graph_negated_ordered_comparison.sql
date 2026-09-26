-- `NOT (a < b)` is not `a >= b` when an argument can be a `NaN`, so the CNF conversion keeps the `NOT`
-- on an ordered comparison. The comparison graph built from the constraints stores plain relations and
-- cannot hold a negated one, so such a constraint has to be left out of the graph instead of being taken
-- in as if it were positive - which used to state the opposite of what the constraint says.

DROP TABLE IF EXISTS t_constraint_graph_int;
DROP TABLE IF EXISTS t_constraint_graph_float;

-- The AST stage has no types, so it leaves an ordered comparison alone whatever the column type is.
-- Building the graph for this table used to be an exception.
CREATE TABLE t_constraint_graph_int (a Int64, b Int64, CONSTRAINT c ASSUME NOT (a < b))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_constraint_graph_int VALUES (3, 3), (5, 4), (10, 1);

SELECT count() FROM t_constraint_graph_int WHERE a < b
SETTINGS optimize_using_constraints = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_append_index = 1;

SELECT count() FROM t_constraint_graph_int WHERE a >= b
SETTINGS optimize_using_constraints = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_append_index = 1;

-- The analyzer knows the types, so it keeps the `NOT` only where an argument can really be a `NaN`.
CREATE TABLE t_constraint_graph_float (a Float64, b Float64, CONSTRAINT c ASSUME NOT (a < b))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_constraint_graph_float VALUES (3, 3), (5, 4), (10, 1), (nan, 1);

SELECT count() FROM t_constraint_graph_float WHERE a < b
SETTINGS optimize_using_constraints = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_append_index = 1;

SELECT count() FROM t_constraint_graph_float WHERE a >= b
SETTINGS optimize_using_constraints = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_append_index = 1;

-- The negation is dropped from the graph, not from the query: the condition is still evaluated.
SELECT a, b FROM t_constraint_graph_float WHERE NOT (a < b) ORDER BY a
SETTINGS optimize_using_constraints = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_append_index = 1;

-- The shape the fuzzer hit: a negated `lessOrEquals` in the `ASSUME` constraint of a temporary table.
CREATE TEMPORARY TABLE t_constraint_graph_temporary (a Int64, b Int64, CONSTRAINT c ASSUME NOT (a <= b))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_constraint_graph_temporary VALUES (5, 4), (10, 1);

SELECT count() FROM t_constraint_graph_temporary WHERE a > b
SETTINGS optimize_using_constraints = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_append_index = 1;

-- A comparison that is not negated still reaches the graph and is still used to answer the query
-- without reading the table.
DROP TABLE IF EXISTS t_constraint_graph_positive;
CREATE TABLE t_constraint_graph_positive (a Int64, b Int64, CONSTRAINT c ASSUME a > b)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_constraint_graph_positive VALUES (5, 4), (10, 1);

SELECT count() FROM t_constraint_graph_positive WHERE a > b
SETTINGS optimize_using_constraints = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_append_index = 1;

DROP TABLE t_constraint_graph_int;
DROP TABLE t_constraint_graph_float;
DROP TABLE t_constraint_graph_positive;
