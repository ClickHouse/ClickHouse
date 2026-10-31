-- Tags: no-parallel-replicas
-- no-parallel-replicas: query_log assertions must observe the local execution path.

SET secondary_indices_enable_bulk_filtering = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_minmax_typed_decimal;
DROP TABLE IF EXISTS t_minmax_typed_datetime64;

CREATE TABLE t_minmax_typed_decimal
(
    x Decimal64(1),
    INDEX idx_x x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_minmax_typed_decimal VALUES (33.3), (33.4);

-- Cross-scale Decimal comparisons require a transformed KeyCondition atom, so bulk
-- evaluation must fall back rather than narrowing the bound to Decimal64(1).
SELECT 'decimal cross-scale parity',
    (SELECT count() FROM t_minmax_typed_decimal WHERE x < toDecimal64('33.33', 2)
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_minmax_typed_decimal WHERE x < toDecimal64('33.33', 2)
         SETTINGS use_minmax_index_bulk_filtering = 1, compile_expressions = 0),
    (SELECT count() FROM t_minmax_typed_decimal WHERE x < toDecimal64('33.33', 2));

SELECT count() FROM t_minmax_typed_decimal WHERE x < toDecimal64('33.33', 2)
SETTINGS use_minmax_index_bulk_filtering = 1, compile_expressions = 0,
         log_comment = '04771_decimal_typed_bound'
FORMAT Null;

CREATE TABLE t_minmax_typed_datetime64
(
    t DateTime64(3, 'UTC'),
    INDEX idx_t t TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_minmax_typed_datetime64 VALUES
    (toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC')),
    (toDateTime64('2024-01-01 00:00:00.124', 3, 'UTC'));

-- The scale-4 bound must remain exact: .123 is less than .1235, while .124 is not.
SELECT 'datetime64 cross-scale parity',
    (SELECT count() FROM t_minmax_typed_datetime64
     WHERE t < toDateTime64('2024-01-01 00:00:00.1235', 4, 'UTC')
     SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_minmax_typed_datetime64
     WHERE t < toDateTime64('2024-01-01 00:00:00.1235', 4, 'UTC')
     SETTINGS use_minmax_index_bulk_filtering = 1, compile_expressions = 0),
    (SELECT count() FROM t_minmax_typed_datetime64
     WHERE t < toDateTime64('2024-01-01 00:00:00.1235', 4, 'UTC'));

SELECT count() FROM t_minmax_typed_datetime64
WHERE t < toDateTime64('2024-01-01 00:00:00.1235', 4, 'UTC')
SETTINGS use_minmax_index_bulk_filtering = 1, compile_expressions = 0,
         log_comment = '04771_datetime64_typed_bound'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'decimal fell back', ProfileEvents['IndexBulkFilteringEvaluatedGranules'] = 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04771_decimal_typed_bound'
  AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT 'datetime64 used bulk', ProfileEvents['IndexBulkFilteringEvaluatedGranules'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04771_datetime64_typed_bound'
  AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_minmax_typed_decimal;
DROP TABLE t_minmax_typed_datetime64;
