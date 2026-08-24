DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt64, s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS parts_to_throw_insert = 3, max_parts_to_merge_at_once = 1;

-- The "too many parts" threshold works:
SET max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1;
SYSTEM STOP MERGES test;
INSERT INTO test VALUES (1, 'a');
INSERT INTO test VALUES (2, 'a');
INSERT INTO test VALUES (3, 'a');
INSERT INTO test VALUES (4, 'a'); -- { serverError TOO_MANY_PARTS }

-- But it can be relaxed with a setting:
ALTER TABLE test MODIFY SETTING max_avg_part_size_for_too_many_parts = '1M';

-- It works in the same way if parts are small:
SYSTEM START MERGES test;
OPTIMIZE TABLE test FINAL SETTINGS optimize_throw_if_noop=1;
SYSTEM STOP MERGES test;

INSERT INTO test VALUES (5, 'a');
INSERT INTO test VALUES (6, 'a');
INSERT INTO test VALUES (7, 'a'); -- { serverError TOO_MANY_PARTS }

-- But it allows having more parts if their average size is large:
SYSTEM START MERGES test;
OPTIMIZE TABLE test FINAL SETTINGS optimize_throw_if_noop=1;
SYSTEM STOP MERGES test;

SET max_block_size = 65000, min_insert_block_size_rows = 65000, min_insert_block_size_bytes = '1M';
INSERT INTO test SELECT number, randomString(1000) FROM numbers(0, 10000);
INSERT INTO test SELECT number, randomString(1000) FROM numbers(10000, 10000);
INSERT INTO test SELECT number, randomString(1000) FROM numbers(20000, 10000);

SELECT count(), round(avg(bytes), -6) FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active;

DROP TABLE test;
