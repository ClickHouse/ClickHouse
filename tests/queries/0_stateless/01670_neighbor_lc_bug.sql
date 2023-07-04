SELECT
    neighbor(n, -2) AS int,
    neighbor(s, -2) AS str,
    neighbor(lcs, -2) AS lowCstr
FROM
(
    SELECT
        number % 5 AS n,
        toString(n) AS s,
        CAST(s, 'LowCardinality(String)') AS lcs
    FROM numbers(10)
);

drop table if exists neighbor_test;

CREATE TABLE neighbor_test
(
    `rowNr` UInt8,
    `val_string` String,
    `val_low` LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY rowNr;

INSERT INTO neighbor_test VALUES (1, 'String 1', 'String 1'), (2, 'String 1', 'String 1'), (3, 'String 2', 'String 2');

SELECT
    rowNr,
    val_string,
    neighbor(val_string, -1) AS str_m1,
    neighbor(val_string, 1) AS str_p1,
    val_low,
    neighbor(val_low, -1) AS low_m1,
    neighbor(val_low, 1) AS low_p1
FROM
(
    SELECT *
    FROM neighbor_test
    ORDER BY val_string, rowNr
)
ORDER BY rowNr, val_string, str_m1, str_p1, val_low, low_m1, low_p1
format PrettyCompact;

drop table if exists neighbor_test;
