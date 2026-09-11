#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

json_type='JSON(max_dynamic_paths = 0,
    s LowCardinality(String),
    n LowCardinality(Nullable(String)),
    arr Array(Array(LowCardinality(Nullable(String)))),
    m Map(LowCardinality(String), LowCardinality(Nullable(String))),
    t Tuple(tag LowCardinality(String), obj JSON(v LowCardinality(String))),
    items Array(Nullable(JSON(v LowCardinality(String)))))'

$CLICKHOUSE_CLIENT --multiquery <<SQL
DROP TABLE IF EXISTS json_bf_lc_ranges;
CREATE TABLE json_bf_lc_ranges
(
    id UInt64,
    j ${json_type},
    INDEX idx j TYPE jsonbf_v1(skip_paths = ['t.tag'], false_positive_rate = 0.0001) GRANULARITY 2
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES json_bf_lc_ranges;
SQL

# Unequal blocks split index granules and give successive updates nonzero row offsets.
for start in 0 45; do
    $CLICKHOUSE_CLIENT --multiquery <<SQL
INSERT INTO json_bf_lc_ranges
SELECT number, (
    '{"s":"v' || toString(number) || '","n":' || if(number % 2, '"v' || toString(number) || '"', 'null')
    || ',"arr":[[],[null],["v' || toString(number) || '"]]'
    || ',"m":{"k' || toString(number % 3) || '":"v' || toString(number) || '","nil":null}'
    || ',"t":{"tag":"ignored","obj":{"v":"v' || toString(number) || '"}}'
    || ',"items":[null,{"v":"v' || toString(number) || '"},{"v":"tail' || toString(number) || '"}]'
    || ',"shared":{"k' || toString(number % 3) || '":"v' || toString(number) || '"}}')::${json_type}
FROM numbers(${start}, 45)
SETTINGS max_block_size = $((7 + start * 4 / 45)), max_threads = 1,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SQL
done

queries=''
# Probe the second block of each part and a missing value in one scan per path.
for predicate in \
    "j.s IN ('v7', 'v58', 'v91')" \
    "j.n IN ('v7', 'v58', 'v91')" \
    "hasAny(j.arr, [['v7'], ['v58'], ['v91']])" \
    "j.m['k1'] IN ('v7', 'v58', 'v91') SETTINGS optimize_functions_to_subcolumns = 0" \
    "j.t.obj.v IN ('v7', 'v58', 'v91')" \
    "hasAny(j.items[].v, ['tail7', 'tail58', 'tail91'])" \
    "j.shared.k1 = 'v7' OR j.shared.k1 = 'v58' OR j.shared.k1 = 'v91'"; do
    queries+="SELECT arraySort(groupArray(id)) FROM json_bf_lc_ranges WHERE ${predicate};"
done

expected=$($CLICKHOUSE_CLIENT --multiquery --use_skip_indexes=0 --query "$queries")
for stage in insert merge materialize; do
    case "$stage" in
        merge) $CLICKHOUSE_CLIENT --multiquery --query 'SYSTEM START MERGES json_bf_lc_ranges; OPTIMIZE TABLE json_bf_lc_ranges FINAL;' ;;
        materialize) $CLICKHOUSE_CLIENT --query 'ALTER TABLE json_bf_lc_ranges MATERIALIZE INDEX idx SETTINGS mutations_sync = 2' ;;
    esac
    actual=$($CLICKHOUSE_CLIENT --multiquery --force_data_skipping_indices=idx --query "$queries")
    diff -u <(printf '%s\n' "$expected") <(printf '%s\n' "$actual")
    echo "$stage: indexed results match full scan"
done

$CLICKHOUSE_CLIENT --query 'DROP TABLE json_bf_lc_ranges'
