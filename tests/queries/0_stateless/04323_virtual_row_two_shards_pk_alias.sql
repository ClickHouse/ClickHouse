-- Regression test for PR #106654 / STID 2651-3359:
-- Logical error 'Virtual row boundary violated in MergingSortedAlgorithm'.
-- Triggered when `optimize_read_in_order` + `read_in_order_use_virtual_row` are both
-- enabled and the outer SortingStep sits over a Union of read-in-order subtrees that
-- carry their own per-source virtual-row conversions (e.g. a Distributed table with
-- two shards under `distributed_product_mode = 'allow'`). The outer merge had
-- `apply_virtual_row_conversions=false` because no single conversion fits all children;
-- with that flag false, `setVirtualRow` filled any unmatched merge-header column with a
-- default value, and the debug assertion added in PR #106565 caught the resulting boundary
-- violation. The fix gates `rememberVirtualRowBoundary` and `checkVirtualRowBoundary` on
-- `apply_virtual_row_conversions=true`, so meaningless boundaries are no longer compared.

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
