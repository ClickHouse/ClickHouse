-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

DROP TABLE IF EXISTS high_cardinality_primary_key_set;
CREATE TABLE high_cardinality_primary_key_set
(
    d Date,
    u UInt32
) ENGINE = MergeTree()
ORDER BY (u, d)
SETTINGS index_granularity = 1;

INSERT INTO high_cardinality_primary_key_set SELECT today() - number, number FROM numbers(100);

SELECT count() FROM high_cardinality_primary_key_set WHERE (u, d) IN ((0, today()), (1, today()));

EXPLAIN indexes = 1
SELECT count() FROM high_cardinality_primary_key_set WHERE (u, d) IN ((0, today()), (1, today()));
