-- See also 03604_key_condition_set_tuple_bug.sql‎

DROP TABLE IF EXISTS prd_bid_events_simple_no_partition;
CREATE TABLE prd_bid_events_simple_no_partition
(
    `type` LowCardinality(String),
    `timestamp` DateTime64(9)
)
ENGINE = MergeTree()
PRIMARY KEY (timestamp, type)
ORDER BY (timestamp, type);

INSERT INTO prd_bid_events_simple_no_partition
SELECT
    arrayElement([
        'impression',
        'start',
        'firstQuartile',
        'midpoint',
        'thirdQuartile',
        'complete',
        'ad_request',
        'random_value'
    ], 1 + (number % 8)),
    toDateTime64('2025-11-19 14:26:52' - toIntervalDay(number % 30) - toIntervalSecond(number % 86400) - toIntervalMillisecond(number % 1000), 9)
FROM numbers(500000);

SELECT
    type,
    count()
FROM prd_bid_events_simple_no_partition
WHERE date(timestamp) = '2025-11-01'
GROUP BY type
HAVING type = 'ad_request';

SELECT count()
FROM prd_bid_events_simple_no_partition
WHERE (date(timestamp) = '2025-11-01') AND (type = 'ad_request')
SETTINGS optimize_use_implicit_projections = 0;

SELECT
    count()
FROM prd_bid_events_simple_no_partition
WHERE (date(timestamp) = '2025-11-01') AND (type = 'ad_request')
SETTINGS optimize_use_implicit_projections = 1;
