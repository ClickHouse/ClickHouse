-- A source part that does not store a column with TTL has no values of it, so it does not prevent the column
-- from being dropped as fully expired by a merge with other parts in which it has fully expired.
-- With `ttl_only_drop_columns`, this is the only way such a merge clears the expired values.
-- Background TTL merges are disabled (`max_number_of_merges_with_ttl_in_pool = 0`), `OPTIMIZE` merges instead.

SET optimize_throw_if_noop = 1;

-- The part predates `ADD COLUMN`.
DROP TABLE IF EXISTS t_ttl_absent_added;

CREATE TABLE t_ttl_absent_added
(
    d Date,
    key UInt64
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ttl_only_drop_columns = 1, max_number_of_merges_with_ttl_in_pool = 0;

SYSTEM STOP MERGES t_ttl_absent_added;

INSERT INTO t_ttl_absent_added VALUES ('2020-01-01', 1);
ALTER TABLE t_ttl_absent_added ADD COLUMN value String TTL d + INTERVAL 1 DAY;
INSERT INTO t_ttl_absent_added VALUES ('2020-01-01', 2, 'expired');

SYSTEM START MERGES t_ttl_absent_added;

SELECT 'part without the added column';
OPTIMIZE TABLE t_ttl_absent_added FINAL;
SELECT key, value FROM t_ttl_absent_added ORDER BY key;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_absent_added' AND active;
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ttl_absent_added' AND active AND column = 'value';

DROP TABLE t_ttl_absent_added;

-- The column has already been dropped from the part as fully expired, so the part has no TTL info of it.
DROP TABLE IF EXISTS t_ttl_absent_dropped;

CREATE TABLE t_ttl_absent_dropped
(
    d Date,
    key UInt64,
    value String TTL d + INTERVAL 1 DAY
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ttl_only_drop_columns = 1, max_number_of_merges_with_ttl_in_pool = 0;

INSERT INTO t_ttl_absent_dropped VALUES ('2020-01-01', 1, 'expired');
OPTIMIZE TABLE t_ttl_absent_dropped FINAL;

SELECT 'part with the column dropped';
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ttl_absent_dropped' AND active AND column = 'value';

SYSTEM STOP MERGES t_ttl_absent_dropped;
INSERT INTO t_ttl_absent_dropped VALUES ('2020-01-01', 2, 'expired');
SYSTEM START MERGES t_ttl_absent_dropped;

OPTIMIZE TABLE t_ttl_absent_dropped FINAL;
SELECT key, value FROM t_ttl_absent_dropped ORDER BY key;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_absent_dropped' AND active;
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ttl_absent_dropped' AND active AND column = 'value';

-- The column has not fully expired in one of the parts that store it, so it is kept, with all its values.
SYSTEM STOP MERGES t_ttl_absent_dropped;
INSERT INTO t_ttl_absent_dropped VALUES ('2020-01-01', 3, 'expired');
INSERT INTO t_ttl_absent_dropped VALUES ('2100-01-01', 4, 'live');
SYSTEM START MERGES t_ttl_absent_dropped;

SELECT 'column not fully expired in some part';
OPTIMIZE TABLE t_ttl_absent_dropped FINAL;
SELECT key, value FROM t_ttl_absent_dropped ORDER BY key;
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ttl_absent_dropped' AND active AND column = 'value';
CHECK TABLE t_ttl_absent_dropped SETTINGS check_query_single_value_result = 1;

DROP TABLE t_ttl_absent_dropped;
