SET input_format_defaults_for_omitted_fields=1;

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.defaults;
CREATE TABLE test.defaults
(
    x UInt32,
    y UInt32,
    a DEFAULT x + y,
    b Float32 DEFAULT log(1 + x + y),
    c UInt32 DEFAULT 42,
    e MATERIALIZED x + y,
    f ALIAS x + y
) ENGINE = MergeTree ORDER BY x;

INSERT INTO test.defaults FORMAT JSONEachRow {"x":1, "y":1};
INSERT INTO test.defaults (x, y) SELECT x, y FROM test.defaults LIMIT 1;
INSERT INTO test.defaults FORMAT JSONEachRow {"x":2, "y":2, "c":2};
INSERT INTO test.defaults FORMAT JSONEachRow {"x":3, "y":3, "a":3, "b":3, "c":3};
INSERT INTO test.defaults FORMAT JSONEachRow {"x":4} {"y":5, "c":5} {"a":6, "b":6, "c":6};

SELECT * FROM test.defaults ORDER BY (x, y);

ALTER TABLE test.defaults ADD COLUMN n Nested(a UInt64, b String);
INSERT INTO test.defaults FORMAT JSONEachRow {"x":7, "y":7, "n.a":[1,2], "n.b":["a","b"]};
SELECT * FROM test.defaults WHERE x = 7 FORMAT JSONEachRow;

ALTER TABLE test.defaults ADD COLUMN n.c Array(UInt8) DEFAULT arrayMap(x -> 0, n.a) AFTER n.a;
INSERT INTO test.defaults FORMAT JSONEachRow {"x":8, "y":8, "n.a":[3,4], "n.b":["c","d"]};
INSERT INTO test.defaults FORMAT JSONEachRow {"x":9, "y":9};
SELECT * FROM test.defaults WHERE x > 7 ORDER BY x FORMAT JSONEachRow;

DROP TABLE test.defaults;
