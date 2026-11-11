SET enable_analyzer = 1;

DROP TABLE IF EXISTS table_with_materialized;
CREATE TABLE table_with_materialized (col String MATERIALIZED 'A', ins Int Ephemeral) ENGINE = Memory;
SELECT number FROM numbers(1) AS n, table_with_materialized;
DROP TABLE table_with_materialized;
