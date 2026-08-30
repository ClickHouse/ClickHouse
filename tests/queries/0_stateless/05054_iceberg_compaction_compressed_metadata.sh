#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# A full-table compaction (`writeMetadataFiles`, reached when the history contains position
# deletes) used to write the new `metadata.json` as raw bytes, ignoring the metadata codec that
# `compactIcebergTable` seeds the file-name generator from. The file was therefore named
# `vN.gz.metadata.json` but held plain JSON, and the next read failed once
# `getCompressionMethodFromMetadataFile` inferred `gzip` from the suffix and tried to decompress it.
#
# `04815_iceberg_optimize_position_delete_row_count_113745` covers the same compaction path with
# the default (uncompressed) metadata, where the raw write happens to be correct.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

TABLE="t_${CLICKHOUSE_DATABASE}_gz"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --iceberg_metadata_compression_method='gzip' --query "
    CREATE TABLE ${TABLE} (id Int64, data String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_compression_method='gzip' --query \
    "INSERT INTO ${TABLE} SELECT number, char(number + ascii('a')) FROM numbers(10, 90)"
# The position delete file is what makes `OPTIMIZE` take the full-table rewrite path.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_compression_method='gzip' --mutations_sync=2 --query \
    "ALTER TABLE ${TABLE} DELETE WHERE id < 20"

optimize_err=$(${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --iceberg_metadata_compression_method='gzip' --query \
    "OPTIMIZE TABLE ${TABLE}" 2>&1)

if [[ "${IS_CLOUD}" != "1" && -n "${optimize_err}" ]]; then
    echo "FAIL: OPTIMIZE failed on the open-source build: ${optimize_err}"
else
    echo "OPTIMIZE accepted the position delete snapshot"
fi

# The compacted metadata file must be readable again: a plain-bytes file under a `.gz` suffix
# fails here with a decompression error rather than returning rows.
${CLICKHOUSE_CLIENT} --query "SELECT count() = 80, min(id) = 20, max(id) = 99 FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
rm -rf "${TABLE_PATH}" 2>/dev/null
