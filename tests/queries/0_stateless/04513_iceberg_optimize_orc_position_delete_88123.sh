#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/88123:
# an Iceberg table mixing data-file formats, where a non-Parquet data file (`ORC`)
# is newer than all position delete files. Position deletes are applied through
# `IcebergBitmapPositionDeleteTransform`, which requires `ChunkInfoRowNumbers` in
# every chunk, and only the Parquet input formats attach it. A data file with
# attached position deletes is guaranteed to be Parquet, but one without them may
# be in any format, so the transform must be skipped for it.
#
# The `OPTIMIZE TABLE` part of this test was removed together with Iceberg data
# compaction, which no longer runs. The read path applies the same transform per
# data file, which is what is asserted here: the position delete applies to the
# Parquet data file that has one, and the ORC row survives.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

# A Parquet data file, then a position delete file for it.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1), (3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "ALTER TABLE ${TABLE} DELETE WHERE c0 = 1"

# An ORC data file newer than all position delete files: no position deletes
# are attached to it, and the ORC input format does not provide `ChunkInfoRowNumbers`.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO TABLE FUNCTION icebergLocal('${TABLE_PATH}', 'ORC') VALUES (2)"

${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
