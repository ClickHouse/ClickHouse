#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_json_bloom_filter_index=1"

json_type='JSON(max_dynamic_paths = 0,
    m Map(LowCardinality(String), LowCardinality(String)),
    plain Map(String, String),
    nullable Map(LowCardinality(String), LowCardinality(Nullable(String))),
    nested Map(String, Tuple(v String)),
    arr Array(Map(String, String)))'

$CLICKHOUSE_CLIENT --multiquery <<SQL
DROP TABLE IF EXISTS json_bf_map_ranges;
CREATE TABLE json_bf_map_ranges
(
    id UInt64,
    j ${json_type},
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 2
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES json_bf_map_ranges;
SQL

# Unequal blocks split index granules and give successive updates nonzero row offsets.
for start in 0 45; do
    $CLICKHOUSE_CLIENT --multiquery <<SQL
INSERT INTO json_bf_map_ranges
SELECT number, (
    '{"m":' || if(number % 5 = 0, '{}',
        toJSONString(map('k' || toString(number % 3), 'v' || toString(number),
            '', 'empty-key', 'a.b', 'dot', concat('a', char(0), 'b'), 'nul')))
    || ',"plain":{"k' || toString(number % 3) || '":"v' || toString(number) || '"}'
    || ',"nullable":{"k' || toString(number % 3) || '":"v' || toString(number) || '","nil":null}'
    || ',"nested":{"k":{"v":"v' || toString(number) || '"}}'
    || ',"arr":[{}, {"k":"v' || toString(number) || '"}]'
    || ',"shared":"v' || toString(number) || '"}')::${json_type}
FROM numbers(${start}, 45)
SETTINGS max_block_size = $((7 + start * 4 / 45)), max_threads = 1,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SQL
done

$CLICKHOUSE_CLIENT --query "SELECT count() FROM json_bf_map_ranges
    WHERE j.m[concat('a', char(0), 'b')] = 'nul' SETTINGS use_skip_indexes = 0"

queries=''
# Probe the second block of each part and a missing value.
for predicate in \
    "j.m['k1'] IN ('v7', 'v58', 'v91')" \
    "j.m[''] = 'empty-key'" \
    "j.m['a.b'] = 'dot'" \
    "j.m[concat('a', char(0), 'b')] = 'nul'" \
    "j.plain['k1'] IN ('v7', 'v58', 'v91')" \
    "j.nullable['k1'] IN ('v7', 'v58', 'v91')" \
    "j.nested['k'].v IN ('v7', 'v58', 'v91')" \
    "j.shared = 'v58'"; do
    queries+="SELECT arraySort(groupArray(id)) FROM json_bf_map_ranges WHERE ${predicate};"
done

expected=$($CLICKHOUSE_CLIENT --multiquery --use_skip_indexes=0 --query "$queries")
for stage in insert merge materialize; do
    case "$stage" in
        merge) $CLICKHOUSE_CLIENT --multiquery --query 'SYSTEM START MERGES json_bf_map_ranges; OPTIMIZE TABLE json_bf_map_ranges FINAL;' ;;
        materialize) $CLICKHOUSE_CLIENT --query 'ALTER TABLE json_bf_map_ranges MATERIALIZE INDEX idx SETTINGS mutations_sync = 2' ;;
    esac
    actual=$($CLICKHOUSE_CLIENT --multiquery --optimize_functions_to_subcolumns=0 --force_data_skipping_indices=idx --query "$queries")
    diff -u <(printf '%s\n' "$expected") <(printf '%s\n' "$actual")
    echo "$stage: indexed results match full scan"
done

$CLICKHOUSE_CLIENT --query 'DROP TABLE json_bf_map_ranges'
