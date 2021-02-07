DROP TABLE IF EXISTS foo;

CREATE TABLE foo (
    id UInt32,
    a Float64,
    b Float64,
    c Float64,
    d Float64
) Engine = MergeTree()
  PARTITION BY id
  ORDER BY id;

INSERT INTO foo VALUES (1, 0.5, 0.2, 0.3, 0.8);

SELECT divide(sum(a) + sum(b), nullIf(sum(c) + sum(d), 0)) FROM foo SETTINGS compile_expressions = 1;
SELECT divide(sum(a) + sum(b), nullIf(sum(c) + sum(d), 0)) FROM foo SETTINGS compile_expressions = 1;

DROP TABLE foo;
