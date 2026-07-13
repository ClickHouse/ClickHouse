DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Map(Tuple(Tuple(), Int), Int))
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO TABLE t0 (c0)
VALUES
(
    map(
        ((), 1), 1,
        ((), 1), 1
    )
),
(
    map(
        ((), 1), 1,
        ((), 1), 1
    )
);

SELECT * FROM t0;


DROP TABLE IF EXISTS t1;

CREATE TABLE t1 (c0 Map(Tuple(Tuple(), Int), Int))
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO TABLE t1 (c0)
VALUES
(
    map()
),
(
    map(
        ((), 1), 1,
        ((), 1), 1
    )
);

SELECT * FROM t1;
