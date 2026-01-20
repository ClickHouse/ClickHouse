DROP TABLE IF EXISTS default.tm1;

CREATE TABLE default.tm1
(
    a Map(Tuple(a String, b String), String)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO default.tm1
SELECT mapFromArrays(
    [tuple('kA1','kB1'), tuple('kA2','kB2')],
    ['v1','v2']
);

SELECT *
FROM default.tm1;

SELECT *
FROM default.tm1
INTO OUTFILE 'tm15.parquet'
FORMAT Parquet;

SELECT *
FROM file('tm15.parquet', Parquet);