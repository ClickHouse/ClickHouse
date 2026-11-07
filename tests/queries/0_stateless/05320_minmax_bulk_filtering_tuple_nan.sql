-- Tags: no-parallel-replicas
-- no-parallel-replicas: per-query SETTINGS toggling skip-index evaluation paths
-- must take effect on the executing replica.

-- The bounds of a minmax index over a `Tuple` are built per element, so a granule holding
-- the rows (1, 1) and (1, nan) has the bounds [(1, 1), (1, nan)]. Bulk filtering must not
-- drop such a granule for `t > (1, 0)`, which the row (1, 1) satisfies.

SET secondary_indices_enable_bulk_filtering = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_bulk_tuple_nan;

CREATE TABLE t_bulk_tuple_nan
(
    k UInt32,
    t Tuple(Float64, Float64),
    INDEX idx_t t TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 2;

INSERT INTO t_bulk_tuple_nan VALUES (1, (1, 1)), (2, (1, nan)), (3, (0, 0)), (4, (0, 0));

SELECT count() FROM t_bulk_tuple_nan WHERE t > (1, 0) SETTINGS use_minmax_index_bulk_filtering = 0;
SELECT count() FROM t_bulk_tuple_nan WHERE t > (1, 0) SETTINGS use_minmax_index_bulk_filtering = 1;
SELECT count() FROM t_bulk_tuple_nan WHERE t >= (1, 0.5) SETTINGS use_minmax_index_bulk_filtering = 1;

DROP TABLE t_bulk_tuple_nan;
