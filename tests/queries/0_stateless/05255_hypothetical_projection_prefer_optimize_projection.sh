#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# prefer_optimize_projection makes the optimizer use any usable projection, and the verdict follows it;
# the twin table with the projection materialized shows what the optimizer really reads

PIN="optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, optimize_use_projections = 1"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_prefer; DROP TABLE IF EXISTS t_prefer_real;
    CREATE TABLE t_prefer (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi';
    CREATE TABLE t_prefer_real AS t_prefer;
    ALTER TABLE t_prefer_real ADD PROJECTION p_b (SELECT a, b, v ORDER BY b);
    INSERT INTO t_prefer SELECT number, number % 100, number FROM numbers(300);
    INSERT INTO t_prefer_real SELECT number, number % 100, number FROM numbers(300);
"

check()
{
    local where="$1" settings="$2" session="$3"
    $CLICKHOUSE_CLIENT -q "
        ${session}
        CREATE HYPOTHETICAL PROJECTION p_b ON t_prefer (SELECT a, b, v ORDER BY b);
        EXPLAIN WHATIF SELECT a, b, v FROM t_prefer ${where} SETTINGS ${PIN}${settings};
        EXPLAIN SELECT a, b, v FROM t_prefer_real ${where} SETTINGS ${PIN}${settings};
    " | grep -E '^\s+(status|verdict|reason):|ReadFromMergeTree \(' \
      | sed -E 's/.*ReadFromMergeTree \(p_b\).*/real: from the projection/; s/.*ReadFromMergeTree \(.*/real: from the base table/' \
      | awk '{$1=$1; print}'
}

echo "--- more marks than the base table ---"
check "WHERE a = 42 AND b >= 40" ", prefer_optimize_projection = 0"
check "WHERE a = 42 AND b >= 40" ", prefer_optimize_projection = 1"

echo "--- the same marks and no ORDER BY to serve ---"
check "WHERE a = 42 AND b = 42" ", prefer_optimize_projection = 0"
check "WHERE a = 42 AND b = 42" ", prefer_optimize_projection = 1"

echo "--- no filter and no ORDER BY ---"
check "" ", prefer_optimize_projection = 0"
check "" ", prefer_optimize_projection = 1"

echo "--- a projection that wins on cost stays plainly chosen ---"
check "WHERE b = 42" ", prefer_optimize_projection = 1"

echo "--- the setting from the session ---"
check "WHERE a = 42 AND b >= 40" "" "SET prefer_optimize_projection = 1;"

echo "--- force_optimize_projection does the same ---"
check "WHERE a = 42 AND b >= 40" ", prefer_optimize_projection = 0, force_optimize_projection = 1"
check "WHERE a = 42 AND b >= 40" ", prefer_optimize_projection = 0" "SET force_optimize_projection = 1;"

echo "--- the query setting overrides the session one ---"
check "WHERE a = 42 AND b >= 40" ", prefer_optimize_projection = 0, force_optimize_projection = 0" "SET force_optimize_projection = 1;"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_prefer; DROP TABLE t_prefer_real"
