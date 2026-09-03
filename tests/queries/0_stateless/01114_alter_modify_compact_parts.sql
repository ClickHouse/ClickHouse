DROP TABLE IF EXISTS mt_compact;

CREATE TABLE mt_compact (d Date, id UInt32, s String)
    ENGINE = MergeTree ORDER BY id PARTITION BY d
    SETTINGS min_bytes_for_wide_part = 10000000, index_granularity = 128;

INSERT INTO mt_compact SELECT toDate('2020-01-05'), number, toString(number) FROM numbers(1000);
INSERT INTO mt_compact SELECT toDate('2020-01-06'), number, toString(number) FROM numbers(1000);
ALTER TABLE mt_compact MODIFY COLUMN s UInt64;
SELECT sum(s) from mt_compact;

DROP TABLE IF EXISTS mt_compact;
