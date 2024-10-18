-- https://github.com/ClickHouse/ClickHouse/issues/70022
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (
    id UInt32,
    type String,
    data String
)
ENGINE=MergeTree
ORDER BY tuple();

CREATE TABLE dst (
    id UInt32,
    a Array(
        Tuple (
            col_a Nullable(String),
            type String
        )
    ),
    b Array(
        Tuple (
            col_b Nullable(String),
            type String
        )
    )
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO src VALUES
(1, 'ok', 'data');

INSERT INTO dst (
    id,
    a,
    b
)
SELECT
    id,
    arraySort(
        x-> tupleElement(x, 2),
        groupArrayDistinctIf(
            tuple(
                nullIf(data, '') AS col_a,
                type
            ),
            col_a is NOT NULL
        )
    ) AS a,

    arraySort(
        x-> tupleElement(x, 2),
        groupArrayDistinctIf(
            tuple(
                nullIf(data, '') AS col_b,
                type
            ),
            col_b is NOT NULL
        )
    ) AS b
FROM
    src
GROUP BY id;

SELECT * FROM dst;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
