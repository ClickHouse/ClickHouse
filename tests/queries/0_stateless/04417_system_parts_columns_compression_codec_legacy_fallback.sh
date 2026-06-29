#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

wide_table=test_system_parts_columns_compression_codec_legacy_wide
compact_table=test_system_parts_columns_compression_codec_legacy_compact
metadata_file=column_compression_codecs.txt

${CLICKHOUSE_CLIENT} --multiquery <<SQL
DROP TABLE IF EXISTS ${wide_table};
DROP TABLE IF EXISTS ${compact_table};

CREATE TABLE ${wide_table}
(
    p UInt8,
    v UInt8,
    s String CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY p
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE ${compact_table}
(
    p UInt8,
    v UInt8,
    s String CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY p
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000,
    compress_per_column_in_compact_parts = 1;

INSERT INTO ${wide_table} VALUES (1, 1, 'wide');
INSERT INTO ${compact_table} VALUES (1, 1, 'compact');
SQL

for table in "${wide_table}" "${compact_table}"; do
    path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active")
    if [[ -z "$path" || "$path" == *..* ]]; then
        echo "Unsafe path: $path" >&2
        exit 1
    fi
    rm -f "${path:?}/${metadata_file}"
done

${CLICKHOUSE_CLIENT} --query "
    SELECT 'before_wide', if(compression_codec = '', '<empty>', compression_codec)
    FROM system.parts_columns
    WHERE database = currentDatabase()
        AND table = '${wide_table}'
        AND active
        AND column = 's'";

${CLICKHOUSE_CLIENT} --query "
    SELECT 'before_compact', if(compression_codec = '', '<empty>', compression_codec)
    FROM system.parts_columns
    WHERE database = currentDatabase()
        AND table = '${compact_table}'
        AND active
        AND column = 's'";

${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${wide_table} UPDATE v = v + 1 WHERE p = 1 SETTINGS mutations_sync = 2";
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${compact_table} UPDATE v = v + 1 WHERE p = 1 SETTINGS mutations_sync = 2";

${CLICKHOUSE_CLIENT} --query "
    SELECT 'after_wide', if(compression_codec = '', '<empty>', compression_codec)
    FROM system.parts_columns
    WHERE database = currentDatabase()
        AND table = '${wide_table}'
        AND active
        AND column = 's'";

${CLICKHOUSE_CLIENT} --query "
    SELECT 'after_compact', if(compression_codec = '', '<empty>', compression_codec)
    FROM system.parts_columns
    WHERE database = currentDatabase()
        AND table = '${compact_table}'
        AND active
        AND column = 's'";

${CLICKHOUSE_CLIENT} --multiquery <<SQL
DROP TABLE IF EXISTS ${wide_table};
DROP TABLE IF EXISTS ${compact_table};
SQL
