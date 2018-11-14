SET insert_sample_with_metadata=1;

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.defaults;
CREATE TABLE IF NOT EXISTS test.defaults
(
    x UInt32,
    y UInt32,
    a DEFAULT x + y,
    b Float32 DEFAULT log(1 + x + y),
    c UInt32 DEFAULT 42,
    e MATERIALIZED x + y,
    f ALIAS x + y
) ENGINE = Memory;

INSERT INTO test.defaults FORMAT JSONEachRow {"x":1, "y":1};
INSERT INTO test.defaults (x, y) SELECT x, y FROM test.defaults LIMIT 1;
INSERT INTO test.defaults FORMAT JSONEachRow {"x":2, "y":2, "c":2};
INSERT INTO test.defaults FORMAT JSONEachRow {"x":3, "y":3, "a":3, "b":3, "c":3};
INSERT INTO test.defaults FORMAT JSONEachRow {"x":4} {"y":5, "c":5} {"a":6, "b":6, "c":6};

SELECT * FROM test.defaults ORDER BY (x, y);
DROP TABLE IF EXISTS test.defaults;
