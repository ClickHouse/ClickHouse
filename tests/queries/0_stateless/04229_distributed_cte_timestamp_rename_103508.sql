-- Regression test for #103508:
-- `NOT_FOUND_COLUMN_IN_BLOCK` — the new analyzer renamed `timestamp` to
-- `timestamp_0` inside a CTE wrapping a `Distributed` table when the
-- underlying local table was non-empty, breaking the outer `SELECT`.
-- The bug requires at least one row in the local table to surface; with
-- an empty table the planner short-circuits and no rename happens.
-- The originally reported scenario uses default settings and is fixed on
-- master. Setting `optimize_read_in_order = 0` still hits a related rename
-- in the analyzer, so the regression query pins it to `1` to keep this
-- test stable under random-settings runs.

DROP TABLE IF EXISTS dist_t;
DROP TABLE IF EXISTS dist_t_two_shards;
DROP TABLE IF EXISTS local_t;

CREATE TABLE local_t (timestamp DateTime64(9), id String) ENGINE = MergeTree ORDER BY timestamp;
CREATE TABLE dist_t AS local_t ENGINE = Distributed(test_shard_localhost, currentDatabase(), local_t, rand());
CREATE TABLE dist_t_two_shards AS local_t ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), local_t, rand());

INSERT INTO local_t VALUES ('2024-01-01', 'a');

SELECT 'single shard';
WITH A AS
(
    SELECT * FROM dist_t
    WHERE timestamp >= toDateTime64('2023-01-01', 9)
      AND timestamp <  toDateTime64('2025-01-01', 9)
)
SELECT timestamp, id
FROM A
ORDER BY timestamp AS `timestamp` DESC
LIMIT 10
SETTINGS distributed_product_mode = 'allow', optimize_read_in_order = 1;

SELECT 'two shards';
WITH A AS
(
    SELECT * FROM dist_t_two_shards
    WHERE timestamp >= toDateTime64('2023-01-01', 9)
      AND timestamp <  toDateTime64('2025-01-01', 9)
)
SELECT timestamp, id
FROM A
ORDER BY timestamp AS `timestamp` DESC
LIMIT 10
SETTINGS distributed_product_mode = 'allow', optimize_read_in_order = 1;

DROP TABLE dist_t;
DROP TABLE dist_t_two_shards;
DROP TABLE local_t;
