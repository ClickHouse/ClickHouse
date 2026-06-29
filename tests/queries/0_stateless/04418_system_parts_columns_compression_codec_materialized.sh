#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

table=test_system_parts_columns_compression_codec_materialized

${CLICKHOUSE_CLIENT} --multiquery <<SQL
DROP TABLE IF EXISTS ${table};

CREATE TABLE ${table}
(
    p UInt8,
    s String CODEC(ZSTD(1)),
    m String MATERIALIZED concat(s, '_materialized') CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY p
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000,
    compress_per_column_in_compact_parts = 1;

INSERT INTO ${table} (p, s) VALUES (1, 'old');

ALTER TABLE ${table}
    MODIFY COLUMN m String MATERIALIZED concat(s, '_materialized') CODEC(ZSTD(3));

ALTER TABLE ${table} UPDATE s = 'new' WHERE p = 1 SETTINGS mutations_sync = 2;

SELECT
    column,
    compression_codec
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = '${table}'
    AND active
    AND column IN ('s', 'm')
ORDER BY column;

DROP TABLE ${table};
SQL
