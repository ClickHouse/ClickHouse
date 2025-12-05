SET enable_analyzer = 1;
SET allow_unused_columns_analysis_skipping = 1;

CREATE VIEW test_view (
    x Int32,
    b Int32,
    c Int32
) AS SELECT number as x, x + 1 as b, b * 2 as c FROM numbers(10);

EXPLAIN QUERY TREE SELECT x FROM test_view;
SELECT x FROM test_view;

EXPLAIN QUERY TREE SELECT c FROM test_view;
SELECT c FROM test_view;

DROP VIEW test_view;
