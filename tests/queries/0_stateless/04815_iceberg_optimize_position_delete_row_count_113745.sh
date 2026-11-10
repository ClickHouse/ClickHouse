#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/113745:
# `OPTIMIZE TABLE` on an Iceberg table whose history contains a row delete that
# removed more than one row per position delete file used to throw
# `Unsupported snapshot's operation type OVERWRITE`. The history guard compared
# `added-position-deletes` (a row count) against `added-delete-files` (a file
# count), so it only accepted the accidental one-row-per-file case.
#
# The single-row delete below is a regression control: it is a shape the old guard
# already accepted by coincidence (1 == 1), so it distinguishes nothing about the
# new conjuncts, it only shows they did not break what already worked. The refused
# shapes are covered by `IcebergCompactionOverwriteClassification` in
# `gtest_iceberg_snapshot_summary.cpp`, not here: ClickHouse's own writer emits
# neither equality deletes nor any removal counter, so they cannot be built through
# `IcebergLocal` at all.
#
# The bug lived in the synchronous compaction path (`compactIcebergTable`) used
# by the open-source build. The cloud build routes `OPTIMIZE` through a different
# code path (gated by a member flag rather than the query-level
# `allow_experimental_iceberg_compaction` setting), so there `OPTIMIZE` reports a
# regular user-facing exception instead of running the compaction. It must never
# report the unsupported-operation error on any build; and on the open-source
# build `OPTIMIZE` must additionally succeed, so the path this PR changes is
# actually exercised (a plain failure there would otherwise pass silently).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

count_position_deletes()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '$1' AND content = 'POSITION_DELETE'
    "
}

# $1: label, $2: DELETE predicate, $3: expected row count after OPTIMIZE,
# $4: expected min(id), $5: expected max(id)
run_case()
{
    local label="$1" predicate="$2" rows="$3" min_id="$4" max_id="$5"
    local table="t_${CLICKHOUSE_DATABASE}_${label}"
    local table_path="${USER_FILES_PATH}/${table}/"

    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (id Int64, data String)
        ENGINE = IcebergLocal('${table_path}', 'Parquet')
    "

    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
        "INSERT INTO ${table} SELECT number, char(number + ascii('a')) FROM numbers(10, 90)"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query \
        "ALTER TABLE ${table} DELETE WHERE ${predicate}"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
        "INSERT INTO ${table} SELECT number, char(number + ascii('a')) FROM numbers(100, 10)"

    local pd_before pd_after
    pd_before=$(count_position_deletes "${table}")

    # This used to throw `Unsupported snapshot's operation type OVERWRITE` whenever
    # the delete above removed more than one row per position delete file. Stderr is
    # captured so that the cloud build's user-facing exception does not trip the
    # "having stderror" check.
    local optimize_err
    optimize_err=$(${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query \
        "OPTIMIZE TABLE ${table}" 2>&1)

    if echo "${optimize_err}" | grep -qF "Unsupported snapshot's operation type"; then
        echo "${label} FAIL: OPTIMIZE rejected the position delete snapshot"
    elif [[ "${IS_CLOUD}" = "1" ]]; then
        echo "${label} OPTIMIZE accepted the position delete snapshot"
    elif [[ -n "${optimize_err}" ]]; then
        echo "${label} FAIL: OPTIMIZE failed on the open-source build: ${optimize_err}"
    else
        echo "${label} OPTIMIZE accepted the position delete snapshot"
    fi

    # Reads apply the position delete file whether or not compaction ran, so the row
    # assertions below cannot tell a real compaction from a silent no-op. Compaction
    # rewrites the manifests with data files only, so the delete file must be gone.
    pd_after=$(count_position_deletes "${table}")
    if [[ "${IS_CLOUD}" = "1" ]]; then
        # Compaction does not run here, so there is nothing to assert about its effect.
        echo "${label} position deletes compacted away"
    elif [[ "${pd_before}" -gt 0 && "${pd_after}" -eq 0 ]]; then
        echo "${label} position deletes compacted away"
    else
        echo "${label} FAIL: position delete file not compacted away: before=${pd_before} after=${pd_after}"
    fi

    # The deleted rows must stay deleted and the surviving ones must stay readable,
    # so a fix that admits the snapshot but resurrects or loses rows fails here.
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '${label}', count() = ${rows}, min(id) = ${min_id}, max(id) = ${max_id}
        FROM ${table}
    "

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table} SYNC"
    rm -rf "${table_path}" 2>/dev/null
}

# 10 rows deleted through a single position delete file: the reported defect.
run_case multi_row "id < 20" 90 20 109
# 1 row deleted: satisfied the old guard by coincidence, must stay working.
run_case single_row "id = 11" 99 10 109
