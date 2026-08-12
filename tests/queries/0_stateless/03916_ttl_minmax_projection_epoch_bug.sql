

DROP TABLE IF EXISTS test_ttl_minmax_epoch;

CREATE TABLE test_ttl_minmax_epoch
(
    timestamp DateTime64(9, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp)
TTL timestamp + INTERVAL 1 MINUTE SETTINGS index_granularity = 1;

-- rows from ~1-60 seconds ago, some will expire during merge
INSERT INTO test_ttl_minmax_epoch
SELECT
    now64(9, 'UTC') - toIntervalSecond(1 + rand() % 60) AS timestamp
FROM numbers(1000);

OPTIMIZE TABLE test_ttl_minmax_epoch FINAL;

SELECT (SELECT min(timestamp) FROM test_ttl_minmax_epoch) =
       (SELECT min(timestamp) FROM test_ttl_minmax_epoch SETTINGS optimize_use_implicit_projections = 0);

-- Empty parts (all rows expired by TTL) may have epoch min_time because
-- MinMaxIndex is never initialized; this is expected. Only check non-empty parts.
SELECT countIf(min_time < '1971-01-01') AS parts_with_epoch_mintime
FROM system.parts
WHERE table = 'test_ttl_minmax_epoch' AND database = currentDatabase() AND active AND rows > 0;

DROP TABLE test_ttl_minmax_epoch;
