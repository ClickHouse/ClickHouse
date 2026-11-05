-- Regression test for STID 2651-3359 (debug assertion added in #106565).

DROP TABLE IF EXISTS local_t;
DROP TABLE IF EXISTS dist_t_two_shards;

CREATE TABLE local_t (timestamp DateTime64(9), id String) ENGINE = MergeTree ORDER BY timestamp;
CREATE TABLE dist_t_two_shards AS local_t ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), local_t, rand());

INSERT INTO local_t VALUES ('2024-01-01', 'a');

SELECT 'two shards desc';
WITH A AS
(
    SELECT * FROM dist_t_two_shards
    WHERE timestamp >= toDateTime64('2023-01-01', 9)
      AND timestamp <  toDateTime64('2025-01-01', 9)
)
SELECT timestamp, id
FROM A
ORDER BY timestamp DESC
LIMIT 10
SETTINGS distributed_product_mode = 'allow', optimize_read_in_order = 1, read_in_order_use_virtual_row = 1;

SELECT 'two shards asc';
WITH A AS
(
    SELECT * FROM dist_t_two_shards
    WHERE timestamp >= toDateTime64('2023-01-01', 9)
      AND timestamp <  toDateTime64('2025-01-01', 9)
)
SELECT timestamp, id
FROM A
ORDER BY timestamp ASC
LIMIT 10
SETTINGS distributed_product_mode = 'allow', optimize_read_in_order = 1, read_in_order_use_virtual_row = 1;

SELECT 'two shards desc per-block';
WITH A AS
(
    SELECT * FROM dist_t_two_shards
    WHERE timestamp >= toDateTime64('2023-01-01', 9)
      AND timestamp <  toDateTime64('2025-01-01', 9)
)
SELECT timestamp, id
FROM A
ORDER BY timestamp DESC
LIMIT 10
SETTINGS distributed_product_mode = 'allow', optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1;

DROP TABLE dist_t_two_shards;
DROP TABLE local_t;
