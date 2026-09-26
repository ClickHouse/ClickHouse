-- { echo ON }

DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `order` int,
    `indexed` int,
    PROJECTION proj
    (
        SELECT _part_offset
        ORDER BY indexed
    )
)
ENGINE = MergeTree
ORDER BY order
TTL if(order = 1, '1970-01-02T00:00:00'::DateTime, '2030-01-01T00:00:00'::DateTime);

INSERT INTO test SELECT 1, 10;

OPTIMIZE TABLE test final;

DROP TABLE test;
