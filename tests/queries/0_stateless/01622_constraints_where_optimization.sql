SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_append_index = 0;

DROP TABLE IF EXISTS t_constraints_where;

CREATE TABLE t_constraints_where(a UInt32, b UInt32, CONSTRAINT c1 ASSUME b >= 5, CONSTRAINT c2 ASSUME b <= 10) ENGINE = Memory;

INSERT INTO t_constraints_where VALUES (1, 7);

EXPLAIN SYNTAX SELECT count() FROM t_constraints_where WHERE b > 15; -- assumption -> 0
EXPLAIN QUERY TREE SELECT count() FROM t_constraints_where WHERE b > 15 SETTINGS allow_experimental_analyzer = 1; -- assumption -> 0
EXPLAIN SYNTAX SELECT count() FROM t_constraints_where WHERE b = 20; -- assumption -> 0
EXPLAIN QUERY TREE SELECT count() FROM t_constraints_where WHERE b = 20 SETTINGS allow_experimental_analyzer = 1; -- assumption -> 0
EXPLAIN SYNTAX SELECT count() FROM t_constraints_where WHERE b < 2; -- assumption -> 0
EXPLAIN QUERY TREE SELECT count() FROM t_constraints_where WHERE b < 2 SETTINGS allow_experimental_analyzer = 1; -- assumption -> 0
EXPLAIN SYNTAX SELECT count() FROM t_constraints_where WHERE b > 20 OR b < 8; -- assumption -> remove (b < 20)
EXPLAIN QUERY TREE SELECT count() FROM t_constraints_where WHERE b > 20 OR b < 8 SETTINGS allow_experimental_analyzer = 1; -- assumption -> remove (b < 20)
EXPLAIN SYNTAX SELECT count() FROM t_constraints_where PREWHERE b > 20 OR b < 8; -- assumption -> remove (b < 20)
EXPLAIN QUERY TREE SELECT count() FROM t_constraints_where PREWHERE b > 20 OR b < 8 SETTINGS allow_experimental_analyzer = 1; -- assumption -> remove (b < 20)

DROP TABLE t_constraints_where;

CREATE TABLE t_constraints_where(a UInt32, b UInt32, CONSTRAINT c1 ASSUME b < 10) ENGINE = Memory;

INSERT INTO t_constraints_where VALUES (1, 7);

EXPLAIN SYNTAX SELECT count() FROM t_constraints_where WHERE b = 1 OR b < 18 OR b > 5; -- assumption -> (b < 20) -> 0;
EXPLAIN QUERY TREE SELECT count() FROM t_constraints_where WHERE b = 1 OR b < 18 OR b > 5 SETTINGS allow_experimental_analyzer = 1; -- assumption -> (b < 20) -> 0;

DROP TABLE t_constraints_where;
