-- { echo }

SET optimize_syntax_fuse_functions = 1;
SET allow_suspicious_low_cardinality_types=1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (`a` Float64, `b` LowCardinality(Nullable(Int8))) ENGINE = Log;

SELECT count(b) * count(b) IGNORE NULLS FROM (SELECT b FROM test);

SELECT avg(b) * 3, (sum(b) + 1) + count(b), count(b) * count(b), count() IGNORE NULLS FROM (SELECT b FROM test);

INSERT INTO test (a, b) VALUES
    (0.0, NULL),
    (1.0, 0),
    (-1.0, 1),
    (3.141592653589793, -1),
    (-2.718281828459045, 127),
    (1e-12, -128),
    (1e12, 42),
    (NaN, NULL),
    (Inf, 7),
    (-Inf, -7);

SELECT avg(b) * 3, (sum(b) + 1) + count(b), count(b) * count(b), count() IGNORE NULLS FROM (SELECT b FROM test);

SELECT sum(b), count(b), sum(a), count(a) FROM (SELECT a, b FROM test);

SELECT sum(a), count(a) FROM ( SELECT CAST(materialize(1), 'LowCardinality(Nullable(Int32))') AS a) t;

SET enable_analyzer = 1;

EXPLAIN SYNTAX run_query_tree_passes=1
SELECT count(b) * count(b) IGNORE NULLS FROM (SELECT b FROM test);

EXPLAIN SYNTAX run_query_tree_passes=1
SELECT avg(b) * 3, (sum(b) + 1) + count(b), count(b) * count(b), count() IGNORE NULLS FROM (SELECT b FROM test);

EXPLAIN SYNTAX run_query_tree_passes=1
SELECT sum(b), count(b), sum(a), count(a) FROM (SELECT a, b FROM test);

EXPLAIN SYNTAX run_query_tree_passes=1
SELECT sum(a), count(a) FROM ( SELECT CAST(materialize(1), 'LowCardinality(Nullable(Int32))') AS a) t;
