#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Nested HiveText output must honor the configured collection-items and map-keys delimiters
# (Hive's LazySimpleSerDe separator list), not derive them from the fields delimiter by
# character arithmetic. Here fields = ',', collection items = ';', map keys = ':'.
HIVE_SETTINGS="input_format_hive_text_fields_delimiter = ',', input_format_hive_text_collection_items_delimiter = ';', input_format_hive_text_map_keys_delimiter = ':'"

# Array, Map and Tuple (with a nested Array) at the top level. The array elements, map entries
# and tuple elements are separated by ';' (the collection-items delimiter), a map key and its
# value by ':' (the map-keys delimiter), and the nested array inside the tuple by ':' again.
${CLICKHOUSE_CLIENT} --query "SELECT [1, 2, 3] AS arr, map('k1', 10, 'k2', 20) AS m, tuple(1, 'a', [7, 8]) AS t FORMAT HiveText SETTINGS ${HIVE_SETTINGS}"

# Array of arrays: the outer level uses the collection-items delimiter ';' and the inner level
# falls through to the map-keys delimiter ':', mirroring Hive's separator list by nesting depth.
${CLICKHOUSE_CLIENT} --query "SELECT [[1, 2], [3, 4]] AS aa, map('a', 'x', 'b', 'y') AS m2 FORMAT HiveText SETTINGS ${HIVE_SETTINGS}"
