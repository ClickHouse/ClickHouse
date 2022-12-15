-- This example is definitely incorrect. The only requirement is that the server should not crash.
-- The reference result can be changed if needed.

DROP TABLE IF EXISTS tst;

CREATE TABLE tst
(
    `id` Int,
    `col` Array(String),
    `col.s.a` Array(LowCardinality(String)),
    `col.s.b` Array(LowCardinality(String))
)
ENGINE = ReplacingMergeTree(id)
PARTITION BY tuple()
ORDER BY id;

INSERT INTO tst (id, col, `col.s.a`, `col.s.b`) SELECT
    number,
    ['a', 'b', 'c', 'd'],
    [number],
    [number]
FROM system.numbers
LIMIT 1;

INSERT INTO tst (id, col, `col.s.a`, `col.s.b`) SELECT
    number,
    ['a', 'b', 'c', 'd'],
    [number],
    [number]
FROM numbers_mt(100000);

SELECT *
FROM tst
ORDER BY id
LIMIT 3;

DROP TABLE tst;
