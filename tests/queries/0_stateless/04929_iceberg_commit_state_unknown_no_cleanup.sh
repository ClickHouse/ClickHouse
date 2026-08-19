#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: Iceberg pulls in extra dependencies.
# Tag no-parallel: toggles process-global failpoints.

# The commit logs the injected failure before deciding what it means, and the client streams server
# log lines to its stderr, where the harness reads any content as a test failure.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Iceberg commit writes v<N>.metadata.json conditionally. A response lost after the store
# accepted the object is indistinguishable, at the error, from another writer having taken the
# version. Treating it as the latter deletes the manifests, manifest list and data files of the
# snapshot v<N>.metadata.json now makes current, leaving the table unreadable (issue #112081).
# iceberg_metadata_commit_response_lost throws right after the store accepted the write, so the
# ambiguous state is reachable without an object store.

TABLE_INSERT="t_insert_${CLICKHOUSE_DATABASE}"
TABLE_DELETE="t_delete_${CLICKHOUSE_DATABASE}"
TABLE_DELETE_UNKNOWN="t_delete_unknown_${CLICKHOUSE_DATABASE}"
TABLE_UNKNOWN="t_unknown_${CLICKHOUSE_DATABASE}"
TABLE_OPTIMIZE="t_optimize_${CLICKHOUSE_DATABASE}"
TABLE_OPTIMIZE_UNKNOWN="t_optimize_unknown_${CLICKHOUSE_DATABASE}"
TABLE_HINT="t_hint_${CLICKHOUSE_DATABASE}"
TABLE_CONTROL="t_control_${CLICKHOUSE_DATABASE}"
ALL_TABLES=(
    "${TABLE_INSERT}" "${TABLE_DELETE}" "${TABLE_DELETE_UNKNOWN}" "${TABLE_UNKNOWN}"
    "${TABLE_OPTIMIZE}" "${TABLE_OPTIMIZE_UNKNOWN}" "${TABLE_HINT}" "${TABLE_CONTROL}"
)

cleanup() {
    for t in "${ALL_TABLES[@]}"; do
        rm -rf "${USER_FILES_PATH:?}/${t}" 2>/dev/null
    done
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_commit_response_lost" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_commit_reconcile_fail" 2>/dev/null
}
trap cleanup EXIT

create_and_seed() {
    local table="$1"
    local table_path="${USER_FILES_PATH}/${table}/"
    rm -rf "${table_path}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (c0 Int32)
        ENGINE = IcebergLocal('${table_path}', 'Parquet')
        ORDER BY c0
    "
    # Two data files, so the retry path has previous snapshots to carry forward.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} VALUES (1), (2), (3)"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} VALUES (4), (5), (6)"
}

# Whether the manifest list of the metadata file the readers will pick up is actually stored.
# A missing one is the corruption: readers resolve it and get a file-not-found.
current_manifest_list_present() {
    local table_path="$1"
    python3 - "${table_path}" <<'PY'
import glob, json, os, re, sys
metadata_dir = os.path.join(sys.argv[1], "metadata")
latest, latest_version = None, -1
for path in glob.glob(os.path.join(metadata_dir, "v*.metadata.json")):
    match = re.fullmatch(r"v(\d+)\.metadata\.json", os.path.basename(path))
    if match and int(match.group(1)) > latest_version:
        latest, latest_version = path, int(match.group(1))
if latest is None:
    print("no metadata")
    sys.exit(0)
metadata = json.load(open(latest))
current = metadata.get("current-snapshot-id")
for snapshot in metadata.get("snapshots", []):
    if snapshot.get("snapshot-id") == current:
        print("yes" if os.path.exists(snapshot.get("manifest-list", "")) else "no")
        sys.exit(0)
print("no snapshot")
PY
}

# Exact number of stored files of one kind. Absolute, not relative to a previous count: a relative
# assertion is satisfied by a single survivor and cannot tell "nothing was deleted" from "almost
# everything was deleted". One INSERT of one block writes exactly one data file and two Avro files
# (its manifest and its manifest list), and none of the randomized settings changes that.
stored_files() {
    find "$1" -name "$2" | wc -l
}

# A scan that has to open the data files. count() alone is answered out of the manifests'
# record_count (IcebergMetadata::totalRows), so it returns the right number with every Parquet file
# deleted; summing a column cannot be.
scan_sum() {
    ${CLICKHOUSE_CLIENT} --query "SELECT sum(c0) FROM $1"
}

seed_for_compaction() {
    local table="$1"
    local table_path="${USER_FILES_PATH}/${table}/"
    rm -rf "${table_path}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (c0 Int32)
        ENGINE = IcebergLocal('${table_path}', 'Parquet')
        ORDER BY c0
    "
    # One manifest per INSERT, enough of them to put the manifest list above the threshold used below.
    local inserts
    inserts=$(for i in $(seq 1 8); do echo "INSERT INTO ${table} VALUES (${i});"; done)
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "${inserts}"
}

# Compaction rewrites the manifest layer and leaves the data files alone, so every one of them must
# still be there and the sum must be unchanged whichever way the commit's outcome resolved.
report_compacted_table() {
    local table="$1"
    local table_path="${USER_FILES_PATH}/${table}/"
    current_manifest_list_present "${table_path}"
    echo "data files: $(stored_files "${table_path}" '*.parquet')"
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT sum(c0) FROM ${table}"
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 \
        --query "SELECT sum(c0) FROM icebergLocal('${table_path}', 'Parquet')"
}

echo "-- INSERT: a commit that landed upstream must be reported as committed"
create_and_seed "${TABLE_INSERT}"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_commit_response_lost"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE_INSERT} VALUES (7), (8), (9)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_INSERT}"
scan_sum "${TABLE_INSERT}"
current_manifest_list_present "${USER_FILES_PATH}/${TABLE_INSERT}/"
echo "data files: $(stored_files "${USER_FILES_PATH}/${TABLE_INSERT}/" '*.parquet')"
echo "avro files: $(stored_files "${USER_FILES_PATH}/${TABLE_INSERT}/" '*.avro')"
# A fresh reader over the same path resolves the metadata from scratch, with no cached state.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${USER_FILES_PATH}/${TABLE_INSERT}/', 'Parquet')"
scan_sum "icebergLocal('${USER_FILES_PATH}/${TABLE_INSERT}/', 'Parquet')"

echo "-- DELETE: the mutation commit path has the same shape"
create_and_seed "${TABLE_DELETE}"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_commit_response_lost"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "DELETE FROM ${TABLE_DELETE} WHERE c0 < 3"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_DELETE}"
scan_sum "${TABLE_DELETE}"
current_manifest_list_present "${USER_FILES_PATH}/${TABLE_DELETE}/"
echo "data files: $(stored_files "${USER_FILES_PATH}/${TABLE_DELETE}/" '*.parquet')"

echo "-- DELETE, unknown outcome: the mutation unwind must not delete the rewritten files"
create_and_seed "${TABLE_DELETE_UNKNOWN}"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_commit_response_lost"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_commit_reconcile_fail"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "DELETE FROM ${TABLE_DELETE_UNKNOWN} WHERE c0 < 3" 2>&1 \
    | grep -qF 'UNKNOWN_STATUS_OF_TRANSACTION' && echo "unknown status reported" || echo "NOT REPORTED AS UNKNOWN"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_commit_reconcile_fail"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_commit_response_lost"
current_manifest_list_present "${USER_FILES_PATH}/${TABLE_DELETE_UNKNOWN}/"
echo "data files: $(stored_files "${USER_FILES_PATH}/${TABLE_DELETE_UNKNOWN}/" '*.parquet')"
echo "avro files: $(stored_files "${USER_FILES_PATH}/${TABLE_DELETE_UNKNOWN}/" '*.avro')"
scan_sum "${TABLE_DELETE_UNKNOWN}"
scan_sum "icebergLocal('${USER_FILES_PATH}/${TABLE_DELETE_UNKNOWN}/', 'Parquet')"

echo "-- OPTIMIZE: manifest compaction commits through the same seam"
seed_for_compaction "${TABLE_OPTIMIZE}"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_commit_response_lost"
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --use_iceberg_metadata_files_cache=0 \
    --query "OPTIMIZE TABLE ${TABLE_OPTIMIZE} MANIFEST SETTINGS iceberg_manifest_min_count_to_compact=5"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_commit_response_lost"
report_compacted_table "${TABLE_OPTIMIZE}"

echo "-- OPTIMIZE, unknown outcome: the compaction unwind must not delete the consolidated manifests"
seed_for_compaction "${TABLE_OPTIMIZE_UNKNOWN}"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_commit_response_lost"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_commit_reconcile_fail"
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --use_iceberg_metadata_files_cache=0 \
    --query "OPTIMIZE TABLE ${TABLE_OPTIMIZE_UNKNOWN} MANIFEST SETTINGS iceberg_manifest_min_count_to_compact=5" 2>&1 \
    | grep -qF 'UNKNOWN_STATUS_OF_TRANSACTION' && echo "unknown status reported" || echo "NOT REPORTED AS UNKNOWN"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_commit_reconcile_fail"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_commit_response_lost"
report_compacted_table "${TABLE_OPTIMIZE_UNKNOWN}"

echo "-- Unknown outcome: throws, and leaves every staged file in place"
create_and_seed "${TABLE_UNKNOWN}"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_commit_response_lost"
# Reading the target back is what resolves the ambiguity; with every read failing the outcome
# stays unknown, and an unknown outcome must not license deleting anything.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_commit_reconcile_fail"
# Reported as an unknown outcome rather than a lost race. Presence, not a count: the write is
# attempted once per retry of the enclosing loop, so how many times the code surfaces varies.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE_UNKNOWN} VALUES (7), (8), (9)" 2>&1 \
    | grep -qF 'UNKNOWN_STATUS_OF_TRANSACTION' && echo "unknown status reported" || echo "NOT REPORTED AS UNKNOWN"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_commit_reconcile_fail"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_commit_response_lost"
echo "data files: $(stored_files "${USER_FILES_PATH}/${TABLE_UNKNOWN}/" '*.parquet')"
echo "avro files: $(stored_files "${USER_FILES_PATH}/${TABLE_UNKNOWN}/" '*.avro')"
# Local storage publishes the conditional write before the failpoint fires, so the commit really did
# take effect and its rows are readable. What is asserted is the table surviving the throw: the
# reported failure must not have deleted anything the current snapshot references.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_UNKNOWN}"
scan_sum "${TABLE_UNKNOWN}"

echo "-- version hint: a hint that names no version must not unwind the commit"
HINT_PATH="${USER_FILES_PATH}/${TABLE_HINT}/"
rm -rf "${HINT_PATH}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_HINT}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_HINT} (c0 Int32)
    ENGINE = IcebergLocal('${HINT_PATH}')
    SETTINGS iceberg_use_version_hint = 1
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE_HINT} VALUES (1)"
# Advancing the hint happens after the metadata file is durable. Content that names no version makes
# that step fail, which must stay a logged best-effort failure: escalating it reaches the caller's
# cleanup and deletes the data files of the snapshot that just became current. No failpoint needed.
printf 'not-a-version' > "${HINT_PATH}metadata/version-hint.text"
# The table function resolves metadata by listing, so it reaches the hint only to keep it in sync.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO TABLE FUNCTION icebergLocal('${HINT_PATH}', 'Parquet', 'c0 Int32') (c0) VALUES (2)
"
echo "data files: $(stored_files "${HINT_PATH}" '*.parquet')"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 \
    --query "SELECT sum(c0) FROM icebergLocal('${HINT_PATH}', 'Parquet')"

echo "-- control: with no failpoint armed, sequential commits still work"
CONTROL_PATH="${USER_FILES_PATH}/${TABLE_CONTROL}/"
rm -rf "${CONTROL_PATH}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_CONTROL}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_CONTROL} (c0 Int32)
    ENGINE = IcebergLocal('${CONTROL_PATH}', 'Parquet')
    ORDER BY c0
"
for v in 1 2 3 4; do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE_CONTROL} VALUES (${v})"
done
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_CONTROL}"
scan_sum "${TABLE_CONTROL}"
echo "data files: $(stored_files "${CONTROL_PATH}" '*.parquet')"

for t in "${ALL_TABLES[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${t}"
done
