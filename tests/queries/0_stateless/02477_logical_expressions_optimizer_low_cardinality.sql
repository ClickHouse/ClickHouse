DROP TABLE IF EXISTS t_logical_expressions_optimizer_low_cardinality;
set optimize_min_equality_disjunction_chain_length=3;
CREATE TABLE t_logical_expressions_optimizer_low_cardinality (a LowCardinality(String), b UInt32) ENGINE = Memory;

-- LowCardinality case, ignore optimize_min_equality_disjunction_chain_length limit, optimzer applied
EXPLAIN SYNTAX SELECT a FROM t_logical_expressions_optimizer_low_cardinality WHERE a = 'x' OR a = 'y';
EXPLAIN QUERY TREE SELECT a FROM t_logical_expressions_optimizer_low_cardinality WHERE a = 'x' OR a = 'y' SETTINGS allow_experimental_analyzer = 1;
EXPLAIN SYNTAX SELECT a FROM t_logical_expressions_optimizer_low_cardinality WHERE a = 'x' OR 'y' = a;
EXPLAIN QUERY TREE SELECT a FROM t_logical_expressions_optimizer_low_cardinality WHERE a = 'x' OR 'y' = a SETTINGS allow_experimental_analyzer = 1;
-- Non-LowCardinality case, optimizer not applied for short chains
EXPLAIN SYNTAX SELECT a FROM t_logical_expressions_optimizer_low_cardinality WHERE b = 0 OR b = 1;
EXPLAIN QUERY TREE SELECT a FROM t_logical_expressions_optimizer_low_cardinality WHERE b = 0 OR b = 1 SETTINGS allow_experimental_analyzer = 1;

DROP TABLE t_logical_expressions_optimizer_low_cardinality;
