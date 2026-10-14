#!/usr/bin/env bash
# Patch parts and pending mutations turn off the JOIN runtime filter granule pruning, and the JOIN still sees the rows they change.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="enable_analyzer = 1, enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 1,
    use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, join_runtime_filter_min_probe_rows = 0,
    query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false',
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, make_distributed_plan = 0"

# Prints the JOIN result, then whether the runtime filter considered and dropped any granule.
function join_query()
{
    local out
    out=$($CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 -q "
        SELECT count(), sum(f.v) FROM $1 AS f INNER JOIN rf_dim AS d ON f.id = d.id WHERE d.tag = 'hot'
        SETTINGS $SETTINGS, $2" 2>&1)
    echo -e "$1 $2\t$(grep -v '^\[' <<< "$out")\t$(grep -c '\] RuntimeFilterGranulesConsidered: ' <<< "$out")\t$(grep -c '\] RuntimeFilterGranulesDropped: ' <<< "$out")"
}

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS rf_dim;
DROP TABLE IF EXISTS rf_patch;
DROP TABLE IF EXISTS rf_onfly;
DROP TABLE IF EXISTS rf_alter;
CREATE TABLE rf_dim (id UInt64, tag String) ENGINE = MergeTree ORDER BY id;
INSERT INTO rf_dim SELECT number, if(number < 64, 'hot', 'cold') FROM numbers(2000);
"

for t in rf_patch rf_onfly rf_alter; do
    $CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE $t (k UInt64, id UInt64, v UInt64, w UInt32, INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 16, enable_block_number_column = 1, enable_block_offset_column = 1;
    SYSTEM STOP MERGES $t;
    INSERT INTO $t SELECT number, number, number, number FROM numbers(2000);
    "
done

# 64 cold rows get hot keys, while the minmax of their granules still holds the old keys.
$CLICKHOUSE_CLIENT -q "UPDATE rf_patch SET id = id - 1000 WHERE id >= 1000 AND id < 1064"
$CLICKHOUSE_CLIENT -q "ALTER TABLE rf_onfly UPDATE id = id - 1000 WHERE id >= 1000 AND id < 1064 SETTINGS mutations_sync = 0"
$CLICKHOUSE_CLIENT -q "ALTER TABLE rf_alter MODIFY COLUMN w UInt64 SETTINGS mutations_sync = 0, alter_sync = 0"

join_query rf_patch "apply_patch_parts = 1"
join_query rf_patch "apply_patch_parts = 0"
join_query rf_onfly "apply_mutations_on_fly = 1"
join_query rf_onfly "apply_mutations_on_fly = 0"
join_query rf_alter "apply_patch_parts = 1, apply_mutations_on_fly = 0"
join_query rf_alter "apply_patch_parts = 0, apply_mutations_on_fly = 0"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE rf_dim;
DROP TABLE rf_patch;
DROP TABLE rf_onfly;
DROP TABLE rf_alter;
"
