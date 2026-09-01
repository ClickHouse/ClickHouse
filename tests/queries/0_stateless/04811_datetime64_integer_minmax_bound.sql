DROP TABLE IF EXISTS datetime64_integer_minmax_bound;

CREATE TABLE datetime64_integer_minmax_bound
(
    time DateTime64(9, 'UTC'),
    INDEX idx_time time TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO datetime64_integer_minmax_bound VALUES
    (0::Decimal(9, 2)::DateTime64(9, 'UTC')),
    (0.01::Decimal(9, 2)::DateTime64(9, 'UTC')),
    (0.99::Decimal(9, 2)::DateTime64(9, 'UTC')),
    (1::Decimal(9, 2)::DateTime64(9, 'UTC')),
    (1.01::Decimal(9, 2)::DateTime64(9, 'UTC'));

SELECT 'greater integer, no index', count()
FROM datetime64_integer_minmax_bound
WHERE time > 0
SETTINGS use_skip_indexes = 0;

SELECT 'greater integer, minmax', count()
FROM datetime64_integer_minmax_bound
WHERE time > 0
SETTINGS force_data_skipping_indices = 'idx_time';

SELECT 'less integer, no index', count()
FROM datetime64_integer_minmax_bound
WHERE time < 1
SETTINGS use_skip_indexes = 0;

SELECT 'less integer, minmax', count()
FROM datetime64_integer_minmax_bound
WHERE time < 1
SETTINGS force_data_skipping_indices = 'idx_time';

SELECT 'greater DateTime, minmax', count()
FROM datetime64_integer_minmax_bound
WHERE time > toDateTime(0, 'UTC')
SETTINGS force_data_skipping_indices = 'idx_time';

DROP TABLE datetime64_integer_minmax_bound;
