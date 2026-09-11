#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Preserve runtime `Map` types in both dynamic paths and shared data.
for max_paths in 0 1024; do
    for paths in "[]" "['m.a', 's', 'typed']"; do
        json_type="JSON(max_dynamic_paths = ${max_paths}, typed Map(String, UInt64))"
        $CLICKHOUSE_CLIENT --multiquery <<SQL
DROP TABLE IF EXISTS json_bf_dynamic_map;
CREATE TABLE json_bf_dynamic_map
(
    id UInt64,
    j ${json_type},
    INDEX bf j TYPE jsonbf_v1(include_paths = ${paths}) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO json_bf_dynamic_map
SELECT * FROM format(RowBinary, 'id UInt64, j ${json_type}', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)),
    unhex('03016d'), formatRowNoNewline('RowBinary', CAST(map('k', CAST(tuple(toUInt64(1)) AS Tuple(a UInt64))) AS Dynamic)),
    unhex('0173'), formatRowNoNewline('RowBinary', CAST(map('k', toUInt64(1)) AS Dynamic)),
    unhex('057479706564'), formatRowNoNewline('RowBinary', map('k', toUInt64(1)))))
SETTINGS input_format_binary_read_json_as_string = 0;
SELECT dynamicType(j.m), dynamicType(j.s) FROM json_bf_dynamic_map;
SQL
        for optimize in 0 1; do
            for use_index in 0 1; do
                $CLICKHOUSE_CLIENT --multiquery <<SQL
SET optimize_functions_to_subcolumns = ${optimize};
SET use_skip_indexes = ${use_index};
SELECT groupArray(id) FROM json_bf_dynamic_map WHERE arrayElement(j.m.:\`Map(String, Tuple(a UInt64))\`, 'k').a = 1;
SELECT groupArray(id) FROM json_bf_dynamic_map WHERE j.s.:\`Map(String, UInt64)\`['k'] = 1;
SQL
            done
            # Declared typed `Map` keys still use the index with either optimizer setting.
            $CLICKHOUSE_CLIENT --query "SELECT groupArray(id) FROM json_bf_dynamic_map WHERE j.typed['k'] = 1 SETTINGS optimize_functions_to_subcolumns = ${optimize}, force_data_skipping_indices = 'bf'"
            # Runtime `Map` type hints do not support keyed pruning.
            $CLICKHOUSE_CLIENT --query "SELECT groupArray(id) FROM json_bf_dynamic_map WHERE arrayElement(j.m.:\`Map(String, Tuple(a UInt64))\`, 'k').a = 1 SETTINGS optimize_functions_to_subcolumns = ${optimize}, force_data_skipping_indices = 'bf'" 2>&1 | grep -q 'INDEX_NOT_USED'
            $CLICKHOUSE_CLIENT --query "SELECT groupArray(id) FROM json_bf_dynamic_map WHERE j.s.:\`Map(String, UInt64)\`['k'] = 1 SETTINGS optimize_functions_to_subcolumns = ${optimize}, force_data_skipping_indices = 'bf'" 2>&1 | grep -q 'INDEX_NOT_USED'
        done
        $CLICKHOUSE_CLIENT --query 'DROP TABLE json_bf_dynamic_map'
    done
done
