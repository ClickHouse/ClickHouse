#!/usr/bin/env bash
# Tags: no-fasttest

# ADDED manifest entries written by ClickHouse must leave `sequence_number` and `file_sequence_number` null
# (Iceberg sequence number inheritance), while readers still resolve them from the manifest list.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_t0"
rm -rf "${TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t0 (a Int32, b Int32) ENGINE = IcebergLocal('${TABLE_PATH}/') PARTITION BY (a);
    INSERT INTO t0 VALUES (1, 10), (2, 20);
    INSERT INTO t0 VALUES (1, 11), (3, 30);
"

echo '--- Check that the correct values were written to manifest files ---'
# Manifest files are named `<uuid>.avro`; the glob skips the `snap-*.avro` manifest lists.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        status,
        snapshot_id IN (SELECT snapshot_id FROM system.iceberg_files WHERE database = currentDatabase() AND table = 't0'),
        sequence_number,
        file_sequence_number
    FROM file('${TABLE_PATH}/metadata/????????-????-????-????-????????????.avro', Avro)
    ORDER BY ALL
"

echo '--- Check that on read, we still resolve id and seq numbers correctly ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT partition, sequence_number
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 't0' AND content = 'DATA'
    ORDER BY sequence_number, partition
"

echo '--- Verify correct data ---'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t0 ORDER BY ALL"

rm -rf "${TABLE_PATH}"
