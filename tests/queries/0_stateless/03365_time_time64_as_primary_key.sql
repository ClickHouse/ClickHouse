SET allow_experimental_time_time64_type = 1;
SET use_legacy_to_time = 0;

DROP TABLE IF EXISTS test_time;
CREATE TABLE test_time (a Time, b String) engine=MergeTree order by a;
INSERT INTO test_time SELECT toTime(12 + number), toTime(12 + number)::String FROM numbers(1000000);

SELECT a, b FROM test_time WHERE a > 12435 AND a < 12437;

SELECT
    floor(CAST(a, 'Int32') / 60) AS minute_bucket,
    count(*) AS total_records,
    min(a) AS min_time,
    max(a) AS max_time,
    groupArray(b) AS all_b_values
FROM test_time
WHERE (a > 12435) AND (a < 12437)
GROUP BY minute_bucket
ORDER BY minute_bucket ASC;

DROP TABLE test_time;
