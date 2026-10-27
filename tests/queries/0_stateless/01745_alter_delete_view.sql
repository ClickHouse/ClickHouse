DROP VIEW IF EXISTS test_view;
DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    f1 Int32,
    f2 Int32,
    pk Int32
)
ENGINE = MergeTree()
ORDER BY f1
PARTITION BY pk;

CREATE VIEW test_view AS
SELECT f1, f2
FROM test_table
WHERE pk = 2;

INSERT INTO test_table (f1, f2, pk) VALUES (1,1,1), (1,1,2), (2,1,1), (2,1,2);

SELECT * FROM test_view ORDER BY f1, f2;

ALTER TABLE test_view DELETE WHERE pk = 2; --{serverError NOT_IMPLEMENTED}

SELECT * FROM test_view ORDER BY f1, f2;

DROP VIEW IF EXISTS test_view;
DROP TABLE IF EXISTS test_table;
