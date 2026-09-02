DROP TABLE IF EXISTS sparse_t;

CREATE TABLE sparse_t (
    id UInt64,
    u UInt64,
    s String,
    arr1 Array(String),
    arr2 Array(UInt64),
    t Tuple(a UInt64, s String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;

INSERT INTO sparse_t SELECT
    number,
    if (number % 2 = 0, number, 0),
    if (number % 2 = 0, toString(number), ''),
    if (number % 2 = 0, [''], []),
    if (number % 2 = 0, [0], []),
    (if (number % 2 = 0, number, 0), '')
FROM numbers(2);

-- { echoOn }

SELECT dumpColumnStructure(id) FROM sparse_t;
SELECT dumpColumnStructure(materialize(id)) FROM sparse_t;

SELECT dumpColumnStructure(u) FROM sparse_t;
SELECT dumpColumnStructure(materialize(u)) FROM sparse_t;

SELECT dumpColumnStructure(s) FROM sparse_t;
SELECT dumpColumnStructure(materialize(s)) FROM sparse_t;

SELECT dumpColumnStructure(arr1) FROM sparse_t;
SELECT dumpColumnStructure(materialize(arr1)) FROM sparse_t;

SELECT dumpColumnStructure(arr2) FROM sparse_t;
SELECT dumpColumnStructure(materialize(arr2)) FROM sparse_t;

SELECT dumpColumnStructure(t) FROM sparse_t;
SELECT dumpColumnStructure(materialize(t)) FROM sparse_t;

SELECT dumpColumnStructure(t.a) FROM sparse_t;
SELECT dumpColumnStructure(materialize(t.a)) FROM sparse_t;

SELECT dumpColumnStructure(t.s) FROM sparse_t;
SELECT dumpColumnStructure(materialize(t.s)) FROM sparse_t;

-- { echoOff }


DROP TABLE IF EXISTS sparse_t
;
