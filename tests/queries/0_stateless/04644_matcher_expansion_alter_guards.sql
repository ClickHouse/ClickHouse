-- Guards around the automatic repair mutations that an ALTER queues when column matcher
-- expansion changes the effective expression of a stored column or of a skip index.

SET alter_sync = 2;
SET mutations_sync = 2;

SELECT '-- MATERIALIZED column in the partition key is rejected';

DROP TABLE IF EXISTS t_matcher_guard_partition;

CREATE TABLE t_matcher_guard_partition
(
    a UInt64,
    p UInt64 MATERIALIZED length([COLUMNS('^[ab]$')])
)
ENGINE = MergeTree PARTITION BY p ORDER BY a;

INSERT INTO t_matcher_guard_partition SELECT number FROM numbers(10);

-- `ADD COLUMN b` extends the matcher inside the body of `p`, so `p` would have to be
-- rematerialized. Its stored values are what the parts are partitioned by, so the ALTER must
-- be rejected instead of leaving parts in a partition that no longer matches their data.
ALTER TABLE t_matcher_guard_partition ADD COLUMN b UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT count() FROM t_matcher_guard_partition;

DROP TABLE t_matcher_guard_partition;

SELECT '-- no repair mutation is queued for an empty table';

DROP TABLE IF EXISTS t_matcher_guard_empty;

-- `allow_non_metadata_alters = 0` forbids any ALTER that would rewrite data. On a table
-- without active parts there is nothing to rebuild, so an ALTER that only changes the
-- effective expression of a skip index must stay a pure metadata change.
CREATE TABLE t_matcher_guard_empty
(
    a UInt64,
    x UInt64 ALIAS a + 1,
    INDEX idx x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a;

ALTER TABLE t_matcher_guard_empty MODIFY COLUMN x UInt64 ALIAS a + 2 SETTINGS allow_non_metadata_alters = 0;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_matcher_guard_empty';

SHOW CREATE TABLE t_matcher_guard_empty;

DROP TABLE t_matcher_guard_empty;
