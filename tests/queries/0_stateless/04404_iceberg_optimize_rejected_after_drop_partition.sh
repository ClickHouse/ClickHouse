#!/usr/bin/env bash
# Tags: no-fasttest
#
# Regression test: compaction rebuilds metadata by replaying the snapshot history and
# skips `DELETE` snapshots. A `DELETE` produced by `ALTER TABLE ... DROP PARTITION`
# removes data files, so skipping it would resurrect the dropped partition. Compaction
# only rebuilds when there are position-delete files to materialize (a prior
# `DELETE FROM`), so this test combines a position delete with a partition drop and
# asserts `OPTIMIZE` fails closed instead of bringing the dropped rows back.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_optimize_after_drop"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'x'), (1, 'y'), (1, 'z'), (2, 'keep')"
# Position delete inside partition 1 -> compaction will want to materialize it (need_optimize).
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE a = 1 AND b = 'y'"
# Data-deleting DELETE snapshot from DROP PARTITION.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 2"

echo "--- rows before OPTIMIZE (expect 2: x, z) ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} FORMAT TSV"

echo "--- OPTIMIZE must be rejected (expect NOT_IMPLEMENTED) ---"
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}" 2>&1 \
    | grep -o 'NOT_IMPLEMENTED' | head -1

echo "--- rows after OPTIMIZE (must still be 2; partition 2 not resurrected) ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} FORMAT TSV"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
