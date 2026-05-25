#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME=aggregate_const_keys_after_filter

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_NAME}" >/dev/null
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery <<SQL
CREATE TABLE ${TABLE_NAME}
(
    x UInt32,
    y UInt32,
    z UInt32
)
ENGINE = Log;

INSERT INTO ${TABLE_NAME}
SELECT number, number, number
FROM numbers(1000);
SQL

${CLICKHOUSE_LOCAL} --multiquery --queries-file /dev/stdin <<SQL
SET enable_analyzer = 1;
SET prefer_localhost_replica = 1;
SET optimize_aggregation_in_order = 0;
SET optimize_read_in_order = 0;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_lazy_materialization = 0;
SET distributed_aggregation_memory_efficient = 0;

SELECT *
FROM
(
    SELECT x, sum(y) AS s
    FROM remote('127.0.0.{1,1}:${CLICKHOUSE_PORT_TCP}', '${CLICKHOUSE_DATABASE}', ${TABLE_NAME})
    GROUP BY GROUPING SETS ((x, z + 1), (x, z + 2))
)
WHERE x = 42
ORDER BY ALL;
SQL

${CLICKHOUSE_CLIENT} --multiquery <<SQL
TRUNCATE TABLE ${TABLE_NAME};

INSERT INTO ${TABLE_NAME}
SELECT number % 100, 1, intDiv(number, 100) % 10
FROM numbers(100000);
SQL

${CLICKHOUSE_LOCAL} --multiquery --queries-file /dev/stdin <<SQL
SET enable_analyzer = 1;
SET prefer_localhost_replica = 1;
SET optimize_aggregation_in_order = 0;
SET optimize_read_in_order = 0;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_lazy_materialization = 0;
SET distributed_aggregation_memory_efficient = 1;
SET enable_memory_bound_merging_of_aggregation_results = 1;
SET group_by_two_level_threshold = 1000000000;
SET group_by_two_level_threshold_bytes = 1000000000;

SELECT *
FROM
(
    SELECT x, z, sum(y) AS s
    FROM remote('127.0.0.{1,1}:${CLICKHOUSE_PORT_TCP}', '${CLICKHOUSE_DATABASE}', ${TABLE_NAME})
    GROUP BY x, z
)
WHERE x = 42
ORDER BY ALL;
SQL
