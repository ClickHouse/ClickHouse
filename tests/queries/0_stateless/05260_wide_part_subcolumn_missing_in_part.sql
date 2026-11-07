-- Tags: no-random-merge-tree-settings
-- Random settings limits: min_bytes_for_wide_part=(0, 0)

-- A pending `MODIFY COLUMN y String` makes the metadata snapshot report `String`, so `y.size` is a
-- valid request, while the part on disk still holds `LowCardinality(String)`, whose type has no
-- `size` subcolumn. Reading it anyway resolves the request to an unrelated stream of the parent and
-- yields a column of the wrong class.

DROP TABLE IF EXISTS t_wide;

CREATE TABLE t_wide (x UInt32, y LowCardinality(String)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, string_serialization_version = 'with_size_stream';

INSERT INTO t_wide VALUES (1, 'a'), (2, 'bb');

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_wide' AND active;

SYSTEM STOP MERGES t_wide;
ALTER TABLE t_wide MODIFY COLUMN y String SETTINGS mutations_sync = 0, alter_sync = 0;

-- The subcolumn request is what arms this: without it the parent is read directly.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT x, y.size FROM t_wide ORDER BY x) WHERE explain ILIKE '%y.size%';

-- On disk the column is still LowCardinality while the snapshot says String.
SELECT
    (SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_wide' AND name = 'y'),
    (SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_wide' AND active AND column = 'y');

SELECT x, y.size FROM t_wide ORDER BY x;
SELECT sum(y.size) FROM t_wide;
SELECT count() FROM t_wide WHERE y.size = 2;
SELECT x, y.size, y FROM t_wide ORDER BY x;

-- Compact parts already refused the read; they must keep returning the same values.
DROP TABLE IF EXISTS t_compact;

CREATE TABLE t_compact (x UInt32, y LowCardinality(String)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, string_serialization_version = 'with_size_stream';

INSERT INTO t_compact VALUES (1, 'a'), (2, 'bb');

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;

SYSTEM STOP MERGES t_compact;
ALTER TABLE t_compact MODIFY COLUMN y String SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT x, y.size FROM t_compact ORDER BY x;

-- The same for an array and for a member of a `Nested` column, in both part formats.
DROP TABLE IF EXISTS t_arr_wide;
DROP TABLE IF EXISTS t_arr_compact;
DROP TABLE IF EXISTS t_nested_wide;
DROP TABLE IF EXISTS t_nested_compact;

CREATE TABLE t_arr_wide (x UInt32, arr Array(LowCardinality(String))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, string_serialization_version = 'with_size_stream';
CREATE TABLE t_arr_compact (x UInt32, arr Array(LowCardinality(String))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, string_serialization_version = 'with_size_stream';
CREATE TABLE t_nested_wide (x UInt32, n Nested(y LowCardinality(String))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, string_serialization_version = 'with_size_stream';
CREATE TABLE t_nested_compact (x UInt32, n Nested(y LowCardinality(String))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, string_serialization_version = 'with_size_stream';

INSERT INTO t_arr_wide VALUES (1, ['a', 'bb']), (2, ['ccc']);
INSERT INTO t_arr_compact VALUES (1, ['a', 'bb']), (2, ['ccc']);
INSERT INTO t_nested_wide VALUES (1, ['a', 'bb']), (2, ['ccc']);
INSERT INTO t_nested_compact VALUES (1, ['a', 'bb']), (2, ['ccc']);

SELECT table, part_type FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_arr_wide', 't_arr_compact', 't_nested_wide', 't_nested_compact') AND active
ORDER BY table;

SYSTEM STOP MERGES t_arr_wide;
SYSTEM STOP MERGES t_arr_compact;
SYSTEM STOP MERGES t_nested_wide;
SYSTEM STOP MERGES t_nested_compact;
ALTER TABLE t_arr_wide MODIFY COLUMN arr Array(String) SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_arr_compact MODIFY COLUMN arr Array(String) SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_nested_wide MODIFY COLUMN `n.y` Array(String) SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_nested_compact MODIFY COLUMN `n.y` Array(String) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT table, type FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_arr_wide', 't_arr_compact', 't_nested_wide', 't_nested_compact') AND active
    AND column IN ('arr', 'n.y')
ORDER BY table;

SELECT x, arr.size, arr FROM t_arr_wide ORDER BY x;
SELECT x, arr.size, arr FROM t_arr_compact ORDER BY x;
SELECT x, `n.y`.size, n.y FROM t_nested_wide ORDER BY x;
SELECT x, `n.y`.size, n.y FROM t_nested_compact ORDER BY x;

-- A subcolumn the part's type does have must still be read from its own stream, not derived from
-- the parent: reading only the sizes of long strings must not pull in the data stream.
DROP TABLE IF EXISTS t_sizes;

CREATE TABLE t_sizes (x UInt32, y String) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, string_serialization_version = 'with_size_stream';

INSERT INTO t_sizes SELECT number, repeat('z', 2000) FROM numbers(3000);

-- ast_fuzzer_runs = 0: a fuzzed re-execution inherits log_comment and is logged too, so the
-- stress-test profile would otherwise add rows measuring a different query.
SELECT sum(y.size) FROM t_sizes SETTINGS log_comment = '05260_sizes_only', ast_fuzzer_runs = 0;
SELECT sum(cityHash64(y)) > 0 FROM t_sizes SETTINGS log_comment = '05260_full_data', ast_fuzzer_runs = 0;

SYSTEM FLUSH LOGS query_log;

-- Reading the sizes alone must stay an order of magnitude cheaper than reading the string data.
SELECT
    (SELECT argMax(ProfileEvents['CompressedReadBufferBytes'], event_time_microseconds)
     FROM system.query_log
     WHERE current_database = currentDatabase() AND log_comment = '05260_sizes_only' AND type = 'QueryFinish') * 10
    < (SELECT argMax(ProfileEvents['CompressedReadBufferBytes'], event_time_microseconds)
       FROM system.query_log
       WHERE current_database = currentDatabase() AND log_comment = '05260_full_data' AND type = 'QueryFinish');

DROP TABLE t_sizes;
DROP TABLE t_nested_compact;
DROP TABLE t_nested_wide;
DROP TABLE t_arr_compact;
DROP TABLE t_arr_wide;
DROP TABLE t_compact;
DROP TABLE t_wide;
