DROP TABLE IF EXISTS too_many_parts;
CREATE TABLE too_many_parts (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS parts_to_delay_insert = 5, parts_to_throw_insert = 5;

SYSTEM STOP MERGES too_many_parts;
SET max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
-- Avoid concurrent parts check to avoid flakiness
SET max_threads=1, max_insert_threads=1;

-- exception is not thrown if threshold is exceeded when multi-block INSERT is already started.
-- Single thread is used as different threads check it separately https://github.com/ClickHouse/ClickHouse/issues/61158
INSERT INTO too_many_parts SELECT * FROM numbers(10) SETTINGS max_insert_threads=1;
SELECT count() FROM too_many_parts;

-- exception is thrown if threshold is exceeded on new INSERT.
INSERT INTO too_many_parts SELECT * FROM numbers(10); -- { serverError TOO_MANY_PARTS }

DROP TABLE too_many_parts;
