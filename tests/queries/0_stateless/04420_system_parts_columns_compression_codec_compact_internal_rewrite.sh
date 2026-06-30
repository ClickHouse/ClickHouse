#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

table=test_system_parts_columns_compression_codec_compact_internal_rewrite

${CLICKHOUSE_CLIENT} --multiquery <<SQL
DROP TABLE IF EXISTS ${table};

CREATE TABLE ${table}
(
    p UInt8,
    v UInt8,
    s String CODEC(LZ4)
)
ENGINE = MergeTree
ORDER BY p
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000,
    compress_per_column_in_compact_parts = 1;

INSERT INTO ${table} VALUES (1, 1, 'old');

SELECT
    'before',
    compression_codec
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = '${table}'
    AND active
    AND column = 's';

ALTER TABLE ${table} MODIFY COLUMN s String CODEC(ZSTD(3));
ALTER TABLE ${table} UPDATE v = v + 1 WHERE p = 1 SETTINGS mutations_sync = 2;

SELECT
    'after',
    compression_codec
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = '${table}'
    AND active
    AND column = 's';

DROP TABLE ${table};
SQL
