DROP TABLE IF EXISTS high_cardinality_primary_key_set;
CREATE TABLE high_cardinality_primary_key_set
(
    d Date,
    u UInt32
) ENGINE = MergeTree()
ORDER BY (u, d)
SETTINGS index_granularity = 1;

INSERT INTO high_cardinality_primary_key_set SELECT toDate('2025-01-01') - number, number FROM numbers(100);

SELECT count() FROM high_cardinality_primary_key_set WHERE (u, d) IN ((0, toDate('2025-01-01')), (1, toDate('2025-01-01')));
