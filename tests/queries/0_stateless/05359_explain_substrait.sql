-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

DROP TABLE IF EXISTS t_substrait;
CREATE TABLE t_substrait (id Int64, value Float64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_substrait VALUES (1, 1.5, 'a'), (2, 2.5, 'b');

SELECT 'Gated by enable_substrait';
EXPLAIN SUBSTRAIT SELECT id FROM t_substrait; -- { serverError SUPPORT_IS_DISABLED }

SET enable_substrait = 1;

SELECT 'Read, filter and projection serialize';
EXPLAIN SUBSTRAIT SELECT id + 1 FROM t_substrait WHERE value > 2.0;

SELECT 'A function without a Substrait mapping blocks';
EXPLAIN SUBSTRAIT SELECT cityHash64(name) FROM t_substrait; -- { serverError NOT_IMPLEMENTED }

SELECT 'A LIMIT above i64 blocks, FetchRel cannot hold it';
EXPLAIN SUBSTRAIT SELECT id FROM t_substrait LIMIT 18446744073709551615; -- { serverError NOT_IMPLEMENTED }

SELECT 'DISTINCT over sorted input blocks, AggregateRel does not keep the order';
EXPLAIN SUBSTRAIT SELECT DISTINCT name FROM (SELECT name FROM t_substrait ORDER BY name); -- { serverError NOT_IMPLEMENTED }

SELECT 'The same with a filter between the sort and the DISTINCT';
EXPLAIN SUBSTRAIT SELECT DISTINCT id FROM (SELECT id FROM t_substrait ORDER BY id) WHERE id > 0; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_substrait;
