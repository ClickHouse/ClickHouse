-- { echo }

SET parallel_replicas_local_plan = 1;
SET session_timezone = 'UTC';

-- toDateTime(Date) overflows for Date values beyond ~2106-02-06.
-- The primary key index must not treat toDateTime as monotonic for Date input,
-- otherwise it incorrectly prunes granules containing large Date values.

DROP TABLE IF EXISTS test;
CREATE TABLE test (stamp Date)
ENGINE = MergeTree ORDER BY toDateTime(stamp) SETTINGS index_granularity = 1;

INSERT INTO test SELECT '2024-10-30' FROM numbers(3);
INSERT INTO test SELECT '2024-11-19' FROM numbers(3);
INSERT INTO test SELECT '2149-06-06' FROM numbers(3);

OPTIMIZE TABLE test FINAL;

SELECT count() FROM test WHERE stamp >= '2024-11-01' SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT count() FROM test WHERE stamp >= '2024-11-01';

DROP TABLE test;
