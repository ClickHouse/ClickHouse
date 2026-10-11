DROP TABLE IF EXISTS t_05212_plain SYNC;
DROP VIEW IF EXISTS t_05212_mv SYNC;
DROP TABLE IF EXISTS t_05212_src SYNC;

CREATE TABLE t_05212_src (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_05212_plain (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW t_05212_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM t_05212_src;

BACKUP TABLE t_05212_plain, TABLE t_05212_mv TO Memory('b_05212') FORMAT Null;

DROP TABLE t_05212_plain SYNC;
DROP VIEW t_05212_mv SYNC;

-- A plain MergeTree table rejects an unknown storage policy.
RESTORE TABLE t_05212_plain FROM Memory('b_05212') SETTINGS storage_policy = 'no_such_policy' FORMAT Null; -- { serverError UNKNOWN_POLICY }

-- A materialized view with an inner table keeps its engine in the target definition; the storage policy
-- override must reach the inner table's engine as well, so an unknown policy must be rejected here too.
RESTORE TABLE t_05212_mv FROM Memory('b_05212') SETTINGS storage_policy = 'no_such_policy' FORMAT Null; -- { serverError UNKNOWN_POLICY }

DROP TABLE IF EXISTS t_05212_plain SYNC;
DROP VIEW IF EXISTS t_05212_mv SYNC;
DROP TABLE t_05212_src SYNC;
