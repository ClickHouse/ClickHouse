#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/88123:
# `OPTIMIZE TABLE` on an Iceberg table containing a data file in a non-Parquet
# format (e.g. `ORC`) that is newer than all position delete files used to throw
# `Logical error: 'ChunkInfoRowNumbers does not exist'`. The compaction pipeline
# applied `IcebergBitmapPositionDeleteTransform` to every data file, and the
# transform requires `ChunkInfoRowNumbers` in every chunk, which only the Parquet
# input formats attach. After the fix the transform is skipped for data files
# without attached position deletes, so the compaction succeeds and the position
# deletes are still applied to the Parquet data files that have them.
#
# The bug lived in the synchronous compaction path (`compactIcebergTable` ->
# `writeDataFiles`) used by the open-source build. The cloud build routes
# `OPTIMIZE` through a different background code path (gated by a member flag
# rather than the query-level `allow_experimental_iceberg_compaction` setting),
# so there `OPTIMIZE` reports a regular user-facing exception instead of running
# the compaction. It must never raise a `LOGICAL_ERROR` on any build; and on the
# open-source build `OPTIMIZE` must additionally succeed, so the synchronous
# compaction path this PR changed is actually exercised (a plain failure there
# would otherwise pass silently and leave the fix untested).

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

# On the cloud build `OPTIMIZE` for Iceberg is gated by a member flag rather than
# the query-level `allow_experimental_iceberg_compaction` setting, so it reports a
# user-facing exception instead of running the compaction changed by this PR.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

# This used to throw `Logical error: 'ChunkInfoRowNumbers does not exist'`.
# Capture the client's stderr so a regular user-facing exception on the cloud
# build does not trip the "having stderror" check, then classify the outcome.
OPTIMIZE_ERR=$(${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}" 2>&1)

if echo "${OPTIMIZE_ERR}" | grep -qF 'Logical error'; then
    # The regression: a logical error must never be raised, on any build.
    echo "FAIL: OPTIMIZE crashed with Logical error"
elif [[ "${IS_CLOUD}" = "1" ]]; then
    # Cloud routes `OPTIMIZE` through a background path; a user-facing exception is expected.
    echo "OPTIMIZE did not crash with Logical error"
elif [[ -n "${OPTIMIZE_ERR}" ]]; then
    # Open-source build must run the synchronous compaction path successfully.
    echo "FAIL: OPTIMIZE failed on the open-source build: ${OPTIMIZE_ERR}"
else
    echo "OPTIMIZE did not crash with Logical error"
fi

# The position delete is applied (during compaction on the open-source build, at
# read time on the cloud build) and the ORC row survives it.
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} ORDER BY c0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
