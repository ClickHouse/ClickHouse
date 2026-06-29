#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

table=test_system_parts_columns_compression_codec_vertical_merge

${CLICKHOUSE_CLIENT} --multiquery <<SQL
DROP TABLE IF EXISTS ${table};

CREATE TABLE ${table}
(
    id UInt64,
    key_column String CODEC(ZSTD(1)),
    gathered_column String CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY (id, key_column)
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO ${table} SELECT number, toString(number), repeat('a', 128) FROM numbers(1000);
INSERT INTO ${table} SELECT number + 1000, toString(number), repeat('b', 128) FROM numbers(1000);

OPTIMIZE TABLE ${table} FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT
    column,
    compression_codec
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = '${table}'
    AND active
    AND column IN ('key_column', 'gathered_column')
ORDER BY column;

DROP TABLE ${table};
SQL
