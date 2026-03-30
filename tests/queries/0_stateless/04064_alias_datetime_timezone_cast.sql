-- Tags: no-random-settings

-- Test that ALIAS columns with DateTime/DateTime64 types correctly apply timezone conversion.
-- https://github.com/ClickHouse/ClickHouse/issues/76787

SET session_timezone = 'UTC';

CREATE TABLE timestamp_test (
    timestamp DateTime64(3),
    timestamp_cet DateTime64(3, 'Europe/Berlin') ALIAS timestamp,
    timestamp_minute_cet DateTime64(3, 'Europe/Berlin') ALIAS toStartOfMinute(timestamp_cet),
    timestamp_minute_cet_alt DateTime64(3, 'Europe/Berlin') ALIAS toStartOfMinute(timestamp)
) ENGINE = MergeTree()
ORDER BY timestamp;

INSERT INTO timestamp_test VALUES ('2025-02-25 12:30:30');

SELECT
    timestamp,
    timestamp_cet,
    timestamp_minute_cet,
    timestamp_minute_cet_alt
FROM timestamp_test;

-- Also test with DateTime (not DateTime64)
CREATE TABLE timestamp_test2 (
    ts DateTime,
    ts_berlin DateTime('Europe/Berlin') ALIAS ts
) ENGINE = MergeTree()
ORDER BY ts;

INSERT INTO timestamp_test2 VALUES ('2025-02-25 12:30:30');

SELECT ts, ts_berlin FROM timestamp_test2;

DROP TABLE timestamp_test;
DROP TABLE timestamp_test2;
