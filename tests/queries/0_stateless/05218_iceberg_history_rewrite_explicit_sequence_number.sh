#!/usr/bin/env bash
# Tags: no-fasttest

# Manifests rewritten by `OPTIMIZE` get an explicit `sequence_number` (remapped onto the renumbered history)
# and a null `file_sequence_number`. The plain INSERT case (both null) is covered by 05210.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The cloud build does not run this compaction path.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
if [[ "${IS_CLOUD}" = "1" ]]; then
    cat "${CUR_DIR}/$(basename "${BASH_SOURCE[0]}" .sh).reference"
    exit 0
fi

TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_t0"
rm -rf "${TABLE_PATH}"

# Sequence numbers 1, 2, 3; the rewrite drops the delete snapshot, so the second insert becomes 2.
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    SET mutations_sync = 2;
    CREATE TABLE t0 (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet');
    INSERT INTO t0 VALUES (1), (2), (3);
    ALTER TABLE t0 DELETE WHERE id = 1;
    INSERT INTO t0 VALUES (4);
"

${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE t0"

echo '--- Rewritten manifest entries ---'
# Only the rewritten `<uuid>.avro` manifests remain; the glob skips the `snap-*.avro` manifest lists.
${CLICKHOUSE_CLIENT} --query "
    SELECT status, sequence_number, file_sequence_number
    FROM file('${TABLE_PATH}/metadata/????????-????-????-????-????????????.avro', Avro)
    ORDER BY ALL
"

echo '--- Resolved on read ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT content, sequence_number
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 't0'
    ORDER BY ALL
"

echo '--- Data ---'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t0 ORDER BY id"

rm -rf "${TABLE_PATH}"
