DROP TABLE IF EXISTS agg;

CREATE TABLE agg
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32,
    PROJECTION aaaa
    (
        SELECT
            ts,
            key,
            sum(value)
        GROUP BY ts, key
    )
)
ENGINE = MergeTree
ORDER BY (key, ts);

SET max_bytes_before_external_group_by=1;
SET max_bytes_ratio_before_external_group_by=0;

INSERT INTO agg SELECT 1, toDateTime('2021-12-06 00:00:00') + number, number FROM numbers(100000);

DROP TABLE agg;
