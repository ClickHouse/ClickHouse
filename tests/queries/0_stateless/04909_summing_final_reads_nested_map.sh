#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery <<'EOF'
DROP TABLE IF EXISTS summing_final_nested_map;
CREATE TABLE summing_final_nested_map
(
    v Int64,
    key UInt64,
    hitsMap Nested(name String, value UInt64)
)
ENGINE = SummingMergeTree(v)
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

SYSTEM STOP MERGES summing_final_nested_map;
INSERT INTO summing_final_nested_map VALUES (1, 1, ['clickhouse'], [5]);
INSERT INTO summing_final_nested_map VALUES (-1, 1, ['clickhouse'], [5]);

-- Top-level Nested `...Map` columns use `sumMap` even when they are omitted from
-- `columns_to_sum`, so their non-empty result keeps the row after a real merge.
SELECT count() FROM summing_final_nested_map FINAL;

SYSTEM START MERGES summing_final_nested_map;
OPTIMIZE TABLE summing_final_nested_map FINAL;
SELECT count() FROM summing_final_nested_map;

DROP TABLE summing_final_nested_map;
EOF
