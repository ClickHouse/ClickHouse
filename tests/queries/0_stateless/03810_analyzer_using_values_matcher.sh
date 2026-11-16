#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

func_name="f1_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    SET enable_analyzer = 1;
    DROP FUNCTION IF EXISTS ${func_name};
    DROP TABLE IF EXISTS t1;

    CREATE TABLE t1 (c0 String, c1 Int8) ENGINE = Memory();

    CREATE FUNCTION ${func_name} AS (p0, p1) -> *;

    SELECT 1
    FROM (SELECT 1 as x) AS t0
    LEFT JOIN VALUES (${func_name}(1, 2)) AS t1
    USING (x); -- { serverError UNKNOWN_IDENTIFIER }

    DROP FUNCTION IF EXISTS ${func_name};
    DROP TABLE IF EXISTS t1;
"
