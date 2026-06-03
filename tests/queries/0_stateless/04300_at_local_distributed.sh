#!/usr/bin/env bash
# Tags: distributed
# Regression test: SELECT ts AT LOCAL FROM <distributed table> must work under
# the default allow_nonconst_timezone_arguments = 0.
#
# timeZone() returns the session timezone, which is propagated to remote shards
# with the query settings and is therefore query-wide constant. It is always
# suitable for constant folding (unlike serverTimezone() / hostName() which
# legitimately differ per shard and must still be rejected).

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
        ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_at_local_dist_local, rand());

    -- AT LOCAL on a distributed table must not raise ILLEGAL_COLUMN
    SELECT dt AT LOCAL FROM t_at_local_dist SETTINGS session_timezone = 'UTC' ORDER BY dt;

    -- AT TIME ZONE with a constant string also works
    SELECT dt AT TIME ZONE 'America/Denver' FROM t_at_local_dist ORDER BY dt;

    -- serverTimezone() is shard-specific and must still be rejected under the default setting
    SELECT dt AT TIME ZONE serverTimezone() FROM t_at_local_dist; -- { serverError ILLEGAL_COLUMN }

    DROP TABLE t_at_local_dist;
    DROP TABLE t_at_local_dist_local;
"
