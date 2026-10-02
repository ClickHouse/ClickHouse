#!/usr/bin/env bash
# with no filter, any ORDER BY lets the optimizer take a projection that reads fewer marks, served or not;
# the twin tables with the projection materialized show which read the optimizer really picks

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PIN="optimize_use_implicit_projections = 0, optimize_use_projections = 1, optimize_read_in_order = 1, prefer_optimize_projection = 0, force_optimize_projection = 0, enable_parallel_replicas = 0"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est; DROP TABLE IF EXISTS t_real_coarse; DROP TABLE IF EXISTS t_real_same;
    CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', use_const_adaptive_granularity = 0;
    CREATE TABLE t_real_coarse AS t_est;
    CREATE TABLE t_real_same AS t_est;
    ALTER TABLE t_real_coarse ADD PROJECTION p_b (SELECT a, b, v ORDER BY b) WITH SETTINGS (index_granularity = 1000);
    ALTER TABLE t_real_same ADD PROJECTION p_b (SELECT a, b, v ORDER BY b);
    INSERT INTO t_est SELECT number, number % 100, number FROM numbers(300);
    INSERT INTO t_real_coarse SELECT number, number % 100, number FROM numbers(300);
    INSERT INTO t_real_same SELECT number, number % 100, number FROM numbers(300);
"

# prints the estimate's status and verdict, then which read the optimizer picks on the twin table
check()
{
    local projection_settings="$1" real_table="$2" query="$3"
    $CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b) ${projection_settings};
        EXPLAIN WHATIF ${query//TABLE/t_est};
        EXPLAIN ${query//TABLE/${real_table}};
    " | grep -E '^\s+(status|verdict):|ReadFromMergeTree \(' \
      | sed -E 's/.*ReadFromMergeTree \(p_b\).*/real: from the projection/; s/.*ReadFromMergeTree \(.*/real: from the base table/' \
      | awk '{$1=$1; print}'
}

echo "--- no filter, an ORDER BY the projection order does not serve, fewer marks ---"
check "WITH SETTINGS (index_granularity = 1000)" t_real_coarse "SELECT a, b, v FROM TABLE ORDER BY v SETTINGS ${PIN}"
echo "--- the same with reading in order disabled ---"
check "WITH SETTINGS (index_granularity = 1000)" t_real_coarse "SELECT a, b, v FROM TABLE ORDER BY v SETTINGS ${PIN}, optimize_read_in_order = 0"
echo "--- the same marks ---"
check "" t_real_same "SELECT a, b, v FROM TABLE ORDER BY v SETTINGS ${PIN}"
echo "--- fewer marks win on cost, so prefer_optimize_projection does not force anything ---"
check "WITH SETTINGS (index_granularity = 1000)" t_real_coarse "SELECT a, b, v FROM TABLE ORDER BY v SETTINGS ${PIN}, prefer_optimize_projection = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_est; DROP TABLE t_real_coarse; DROP TABLE t_real_same"
