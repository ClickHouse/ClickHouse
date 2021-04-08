#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

format="$CLICKHOUSE_FORMAT -n"

$format <<EOF
SELECT a, b AS x FROM table AS t
    JOIN table2   AS   t2   ON   (t.id = t2.t_id) 
    WHERE 1 = 1
;
SELECT a, b AS x,
    x == 0 ? a : b
FROM table2 AS t   WHERE t.id  !=  0
EOF
