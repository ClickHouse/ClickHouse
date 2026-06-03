#!/usr/bin/env bash
# Tags: distributed
# Regression test: SELECT ts AT LOCAL FROM <distributed table> must work under
# the default allow_nonconst_timezone_arguments = 0.
# Previously, toTimeZone(expr, timeZone()) raised ILLEGAL_COLUMN in distributed
# mode because timeZone() is a FunctionServerConstantBase that is not
# constant-folded on remote shards when is_distributed = true.

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

    DROP TABLE t_at_local_dist;
    DROP TABLE t_at_local_dist_local;
"
