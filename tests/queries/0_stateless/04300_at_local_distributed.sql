-- Tags: distributed
-- Regression test: AT LOCAL and AT TIME ZONE must work on Distributed tables.

DROP TABLE IF EXISTS t_at_local_dist_local;
DROP TABLE IF EXISTS t_at_local_dist;

CREATE TABLE t_at_local_dist_local (dt DateTime('UTC'))
    ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_at_local_dist_local VALUES ('2001-02-16 20:38:40');

CREATE TABLE t_at_local_dist AS t_at_local_dist_local
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_at_local_dist_local, rand());

-- AT LOCAL works when session_timezone is explicitly set (propagated to all shards)
SELECT dt AT LOCAL FROM t_at_local_dist ORDER BY dt SETTINGS session_timezone = 'UTC';

-- AT TIME ZONE with a constant string always works
SELECT dt AT TIME ZONE 'America/Denver' FROM t_at_local_dist ORDER BY dt;

-- AT LOCAL fails when session_timezone is empty: timeZone() is not constant-foldable
-- in distributed mode without an explicit session_timezone setting
SELECT dt AT LOCAL FROM t_at_local_dist SETTINGS allow_nonconst_timezone_arguments = 0, session_timezone = ''; -- { serverError ILLEGAL_COLUMN }

-- serverTimezone() is shard-specific and must be rejected regardless of session_timezone
SELECT dt AT TIME ZONE serverTimezone() FROM t_at_local_dist SETTINGS allow_nonconst_timezone_arguments = 0, session_timezone = ''; -- { serverError ILLEGAL_COLUMN }

DROP TABLE t_at_local_dist;
DROP TABLE t_at_local_dist_local;
