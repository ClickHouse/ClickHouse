#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$data_path"

$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
SELECT name, value
FROM system.merge_tree_settings
WHERE name IN ('map_serialization_version', 'map_serialization_version_for_zero_level_parts', 'map_max_key_columns')
ORDER BY name;

CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    map_max_key_columns = 0,
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO t VALUES (1, {'a': 1, 'b': 2});
SELECT id, m FROM t ORDER BY id;
"

part_path=$($CLICKHOUSE_LOCAL --path "$data_path" -q "
SELECT path FROM system.parts
WHERE database = currentDatabase() AND table = 't' AND active
ORDER BY name LIMIT 1
")

python3 - <<EOF
import json
from pathlib import Path
path = Path(r'''${part_path}''').joinpath('serialization.json')
data = json.loads(path.read_text())
print(data['types_serialization_versions']['map'])
EOF

$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
DETACH TABLE t;
ATTACH TABLE t;
SELECT id, m FROM t ORDER BY id;
DROP TABLE t;
"

rm -rf "${data_path:?}"
