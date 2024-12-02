DROP TABLE IF EXISTS t_index_granularity_arrays_metric;

CREATE TABLE t_index_granularity_arrays_metric (
    `col` String
)
ENGINE = MergeTree
ORDER BY col;

INSERT INTO t_index_granularity_arrays_metric(col) VALUES ('some-data');

SELECT value > 0 FROM system.metrics WHERE metric = 'MergeTreeIndexGranularityInternalArraysTotalSize';
