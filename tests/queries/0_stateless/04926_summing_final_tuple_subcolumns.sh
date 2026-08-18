#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery <<'EOF'
DROP TABLE IF EXISTS summing_final_tuple_subcolumns;
CREATE TABLE summing_final_tuple_subcolumns
(
    id UInt64,
    metrics Tuple(value Int64, payload LowCardinality(String), nested Tuple(other Int64, note Nullable(String)))
)
ENGINE = SummingMergeTree
ORDER BY id
SETTINGS allow_tuple_element_aggregation = 1;

SYSTEM STOP MERGES summing_final_tuple_subcolumns;
INSERT INTO summing_final_tuple_subcolumns VALUES (1, (1, 'first', (2, 'one'))), (2, (1, 'kept', (0, NULL)));
INSERT INTO summing_final_tuple_subcolumns VALUES (1, (-1, 'second', (-2, 'two'))), (2, (2, 'kept', (0, NULL)));

-- Only the summable tuple leaves participate in the removal decision. The
-- LowCardinality and Nullable siblings must not need to be read by this count.
SELECT count() FROM summing_final_tuple_subcolumns FINAL;

SYSTEM START MERGES summing_final_tuple_subcolumns;
OPTIMIZE TABLE summing_final_tuple_subcolumns FINAL;
SELECT count() FROM summing_final_tuple_subcolumns;

DROP TABLE summing_final_tuple_subcolumns;
EOF
