CREATE TABLE tuple_test
(
    id  UInt64,
    tup Nullable(Tuple(u UInt64, s String)),
    n   Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tuple_test (id, tup, n) VALUES
    (1, tuple(11, 'alpha'), 100),
    (2, NULL,                200),
    (3, tuple(33, 'gamma'),  NULL),
    (4, tuple(44, 'delta'),  400);

SELECT id, tup, n, isNull(tup), isNull(n)
FROM tuple_test
ORDER BY id;
