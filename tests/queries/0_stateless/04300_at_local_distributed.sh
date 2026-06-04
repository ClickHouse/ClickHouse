#!/usr/bin/env bash
# Tags: distributed
# Regression test for AT LOCAL / timeZone() on Distributed tables.
#
# timeZone() is query-wide constant only when session_timezone is explicitly set
# (the setting is propagated to remote shards). When session_timezone is empty,
# the effective timezone falls back to the server's local timezone which may
# differ per shard, so timeZone() keeps per-shard evaluation (old behavior).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_at_local_dist_local;
    DROP TABLE IF EXISTS t_at_local_dist;

    CREATE TABLE t_at_local_dist_local (dt DateTime('UTC'))
        ENGINE = MergeTree ORDER BY dt;
    INSERT INTO t_at_local_dist_local VALUES ('2001-02-16 20:38:40');

    CREATE TABLE t_at_local_dist AS t_at_local_dist_local
        ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_at_local_dist_local, rand());

    -- AT LOCAL works when session_timezone is explicitly set (propagated to all shards)
    SELECT dt AT LOCAL FROM t_at_local_dist SETTINGS session_timezone = 'UTC' ORDER BY dt;

    -- AT TIME ZONE with a constant string always works
    SELECT dt AT TIME ZONE 'America/Denver' FROM t_at_local_dist ORDER BY dt;

    -- AT LOCAL fails when session_timezone is empty: effective timezone is shard-specific
    -- and timeZone() is not constant-foldable in distributed mode without an explicit setting.
    -- Force session_timezone='' explicitly so the test is not affected by runner-injected settings.
    SELECT dt AT LOCAL FROM t_at_local_dist SETTINGS session_timezone = ''; -- { serverError ILLEGAL_COLUMN }

    -- serverTimezone() is shard-specific and must still be rejected regardless of session_timezone
    SELECT dt AT TIME ZONE serverTimezone() FROM t_at_local_dist SETTINGS session_timezone = ''; -- { serverError ILLEGAL_COLUMN }

    DROP TABLE t_at_local_dist;
    DROP TABLE t_at_local_dist_local;
"
