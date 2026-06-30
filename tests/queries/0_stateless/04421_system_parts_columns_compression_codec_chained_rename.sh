#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

table="test_system_parts_columns_compression_codec_chained_rename"

${CLICKHOUSE_CLIENT} --multiquery <<SQL
DROP TABLE IF EXISTS ${table};

CREATE TABLE ${table}
(
    p UInt8,
    a String CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY p
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'LZ4';

INSERT INTO ${table} VALUES (1, 'value');

ALTER TABLE ${table} RENAME COLUMN a TO b SETTINGS mutations_sync = 2;
ALTER TABLE ${table} RENAME COLUMN b TO c SETTINGS mutations_sync = 2;

SELECT
    column,
    compression_codec
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = '${table}'
    AND active
    AND column = 'c';

DROP TABLE ${table};
SQL
