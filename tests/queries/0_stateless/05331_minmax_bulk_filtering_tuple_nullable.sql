-- Tags: no-parallel-replicas
-- no-parallel-replicas: per-query SETTINGS toggling skip-index evaluation paths
-- must take effect on the executing replica.

-- A comparison of `Tuple(Nullable(Int32), Int32)` returns `Nullable(UInt8)`, so bulk filtering
-- of a minmax index over such a key must fall back to the scalar path instead of throwing.

SET secondary_indices_enable_bulk_filtering = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_bulk_tuple_nullable;

CREATE TABLE t_bulk_tuple_nullable
(
    k UInt32,
    t Tuple(Nullable(Int32), Int32),
    INDEX idx_t t TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 2;

INSERT INTO t_bulk_tuple_nullable VALUES (1, (1, 1)), (2, (2, 2)), (3, (10, 10)), (4, (NULL, 11));

SELECT count() FROM t_bulk_tuple_nullable WHERE t > (5, 0) SETTINGS use_minmax_index_bulk_filtering = 0;
SELECT count() FROM t_bulk_tuple_nullable WHERE t > (5, 0) SETTINGS use_minmax_index_bulk_filtering = 1;
SELECT count() FROM t_bulk_tuple_nullable WHERE t <= (2, 2) SETTINGS use_minmax_index_bulk_filtering = 1;

DROP TABLE t_bulk_tuple_nullable;
