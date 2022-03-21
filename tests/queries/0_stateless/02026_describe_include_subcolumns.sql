DROP TABLE IF EXISTS t_desc_subcolumns;

CREATE TABLE t_desc_subcolumns
(
    d Date,
    n Nullable(String) COMMENT 'It is a nullable column',
    arr1 Array(UInt32) CODEC(ZSTD),
    arr2 Array(Array(String)) TTL d + INTERVAL 1 DAY,
    t Tuple(s String, a Array(Tuple(a UInt32, b UInt32))) CODEC(ZSTD)
)
ENGINE = MergeTree ORDER BY d;

DESCRIBE TABLE t_desc_subcolumns FORMAT PrettyCompactNoEscapes;

DESCRIBE TABLE t_desc_subcolumns FORMAT PrettyCompactNoEscapes
SETTINGS describe_include_subcolumns = 1;

DROP TABLE t_desc_subcolumns;
