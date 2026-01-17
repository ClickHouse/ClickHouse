-- Test for issue #84442: ALTER MODIFY ORDER BY does not check if the new column has TTL
-- This test verifies that ALTER TABLE properly validates TTL columns in ORDER BY clauses

CREATE TABLE IF NOT EXISTS test_break_ddl
(
    id String,
    event_date Date,
    event_time DateTime,
    message String
)
ENGINE = ReplacingMergeTree()
PARTITION BY event_date
ORDER BY (id, event_date, event_time);

ALTER TABLE test_break_ddl
    ADD COLUMN `source_address` String TTL event_time + toIntervalDay(30) AFTER event_time,
    ADD COLUMN `destination_address` String TTL event_time + toIntervalDay(30) AFTER source_address,
    MODIFY ORDER BY (id, event_date, event_time, source_address, destination_address); -- { serverError ILLEGAL_COLUMN }

DROP TABLE IF EXISTS test_break_ddl;
