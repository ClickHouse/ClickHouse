SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    f1 Int32,
    f2 Int32,
    pk Int32
) ENGINE = MergeTree PARTITION BY pk ORDER BY f1;

INSERT INTO test_table SELECT number, number, number FROM numbers(10);

DROP VIEW IF EXISTS test_view;
CREATE VIEW test_view AS SELECT f1, f2 FROM test_table WHERE pk = 2;

SELECT * FROM test_view;

DROP VIEW test_view;
DROP TABLE test_table;
