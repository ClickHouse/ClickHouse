DROP TABLE IF EXISTS t_index_agg_func;

CREATE TABLE t_index_agg_func
(
    id UInt64,
    v AggregateFunction(avg, UInt64),
    INDEX idx_v v TYPE minmax GRANULARITY 1
)
ENGINE = AggregatingMergeTree ORDER BY id
SETTINGS index_granularity = 4; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_index_agg_func
(
    id UInt64,
    v AggregateFunction(avg, UInt64),
)
ENGINE = AggregatingMergeTree ORDER BY id
SETTINGS index_granularity = 4;

ALTER TABLE t_index_agg_func ADD INDEX idx_v v TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

ALTER TABLE t_index_agg_func ADD INDEX idx_v finalizeAggregation(v) TYPE minmax GRANULARITY 1;

INSERT INTO t_index_agg_func SELECT number % 10, initializeAggregation('avgState', toUInt64(number % 20)) FROM numbers(1000);
INSERT INTO t_index_agg_func SELECT number % 10, initializeAggregation('avgState', toUInt64(number % 20)) FROM numbers(1000, 1000);

OPTIMIZE TABLE t_index_agg_func FINAL;

SELECT count() FROM system.parts WHERE table = 't_index_agg_func' AND database = currentDatabase() AND active;

SET force_data_skipping_indices = 'idx_v';
SET use_skip_indexes_if_final = 1;

SELECT id, finalizeAggregation(v) AS vv FROM t_index_agg_func FINAL WHERE vv >= 10 ORDER BY id;

DROP TABLE t_index_agg_func;
