#!/usr/bin/env bash
# Tags: no-parallel-replicas

# Projection parts share `name` across parents (every parent part stores its
# projection under the same projection name). Sparsity bitmaps are cached in
# `QueryConditionCache` and shared via `SparseOffsetsShare` keyed by the part
# identity, so the key must include the parent part name to avoid one projection
# part reusing another's bitmap and pruning the wrong granules.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_proj_sparse_identity SYNC"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_proj_sparse_identity
    (
        id UInt64,
        n  UInt32,
        PROJECTION proj_n (SELECT id, n ORDER BY n)
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
             serialization_info_version = 'with_types',
             min_bytes_for_wide_part = 0,
             index_granularity = 1024
"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_proj_sparse_identity"

# Two parent parts whose projection columns are sparse but with different granule
# patterns. Neither is fully default nor fully non-default, so part-level pruning
# does not eliminate either and both reach the granule analyzer.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_proj_sparse_identity SELECT number, if(number < 4500, 0, 1)::UInt32 FROM numbers(5000) SETTINGS optimize_on_insert = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_proj_sparse_identity SELECT number + 5000, if(number < 3500, 0, 1)::UInt32 FROM numbers(5000) SETTINGS optimize_on_insert = 0"

# Truth: 500 ones in part 1 plus 1500 ones in part 2 = 2000.
echo "truth $($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_proj_sparse_identity WHERE n != 0 SETTINGS use_sparsity_info_for_pruning = 'off'")"

# Read through the projection. Use FORMAT JSON and pull the "rows" field, since
# the projection is selected only when the query shape matches its ORDER BY.
proj_rows() {
    local mode=$1
    $CLICKHOUSE_CLIENT -q "
        SYSTEM DROP QUERY CONDITION CACHE;
        SELECT n FROM t_proj_sparse_identity WHERE n != 0 ORDER BY n FORMAT JSON
        SETTINGS optimize_use_projections = 1,
                 use_sparsity_info_for_pruning = '$mode'
    " | python3 -c "import json,sys; print(json.load(sys.stdin)['rows'])"
}

echo "projection_planning $(proj_rows planning)"
echo "projection_data_read $(proj_rows data_read)"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_proj_sparse_identity SYNC"
