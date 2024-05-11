SELECT serial('x');
SELECT serial('x');

DROP TABLE IF EXISTS default.test_table;

CREATE TABLE test_table
(
    CounterID UInt32,
    UserID UInt32,
    ver UInt16
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/1-1/test_table', 'x', ver)
PARTITION BY CounterID
ORDER BY (CounterID, intHash32(UserID))
SAMPLE BY intHash32(UserID);

INSERT INTO test_table VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5);

SELECT *, serial('x') FROM test_table;

SELECT serial('y');