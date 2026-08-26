-- Tags: no-parallel
-- - no-parallel - due to usage of fail points

-- A plain INSERT fans out to `max_insert_threads` parallel sinks. The "too many parts" check must
-- not count the parts committed by the insert's own sibling sinks: an insert that brings the table
-- exactly to `parts_to_throw_insert` used to be spuriously rejected with `TOO_MANY_PARTS` when one
-- of its sinks started late (e.g. on a loaded machine). The failpoint skews the sinks' start times
-- to make the race deterministic.
-- https://github.com/ClickHouse/ClickHouse/issues/114015

DROP TABLE IF EXISTS test_too_many_parts_race;
CREATE TABLE test_too_many_parts_race (x UInt64, s String) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS parts_to_throw_insert = 3, max_parts_to_merge_at_once = 1;

SYSTEM ENABLE FAILPOINT merge_tree_sink_on_start_random_sleep;
SYSTEM STOP MERGES test_too_many_parts_race;

SET max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1;
INSERT INTO test_too_many_parts_race VALUES (1, 'a');
INSERT INTO test_too_many_parts_race VALUES (2, 'a');
INSERT INTO test_too_many_parts_race VALUES (3, 'a');
INSERT INTO test_too_many_parts_race VALUES (4, 'a'); -- { serverError TOO_MANY_PARTS }

SELECT count() FROM test_too_many_parts_race;

SYSTEM DISABLE FAILPOINT merge_tree_sink_on_start_random_sleep;
DROP TABLE test_too_many_parts_race;
