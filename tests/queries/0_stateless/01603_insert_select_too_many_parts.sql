DROP TABLE IF EXISTS too_many_parts;
CREATE TABLE too_many_parts (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS parts_to_delay_insert = 5, parts_to_throw_insert = 5;

SYSTEM STOP MERGES too_many_parts;
SET max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

-- exception is not thrown if threshold is exceeded when multi-block INSERT is already started.
INSERT INTO too_many_parts SELECT * FROM numbers(10);
SELECT count() FROM too_many_parts;

-- exception is thrown if threshold is exceeded on new INSERT.
INSERT INTO too_many_parts SELECT * FROM numbers(10); -- { serverError 252 }

DROP TABLE too_many_parts;
