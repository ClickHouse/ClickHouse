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
TABLE_UNKNOWN="t_unknown_${CLICKHOUSE_DATABASE}"
TABLE_CONTROL="t_control_${CLICKHOUSE_DATABASE}"

cleanup() {
    for t in "${TABLE_INSERT}" "${TABLE_DELETE}" "${TABLE_UNKNOWN}" "${TABLE_CONTROL}"; do
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

echo "-- INSERT: a commit that landed upstream must be reported as committed"
create_and_seed "${TABLE_INSERT}"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_commit_response_lost"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE_INSERT} VALUES (7), (8), (9)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_INSERT}"
current_manifest_list_present "${USER_FILES_PATH}/${TABLE_INSERT}/"
# A fresh reader over the same path resolves the metadata from scratch, with no cached state.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${USER_FILES_PATH}/${TABLE_INSERT}/', 'Parquet')"

echo "-- DELETE: the mutation commit path has the same shape"
create_and_seed "${TABLE_DELETE}"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_commit_response_lost"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "DELETE FROM ${TABLE_DELETE} WHERE c0 < 3"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_DELETE}"
current_manifest_list_present "${USER_FILES_PATH}/${TABLE_DELETE}/"

echo "-- Unknown outcome: throws, and leaves every staged file in place"
create_and_seed "${TABLE_UNKNOWN}"
FILES_BEFORE=$(find "${USER_FILES_PATH}/${TABLE_UNKNOWN}/" -name '*.parquet' -o -name '*.avro' | wc -l)
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
FILES_AFTER=$(find "${USER_FILES_PATH}/${TABLE_UNKNOWN}/" -name '*.parquet' -o -name '*.avro' | wc -l)
# Strictly more files than before: the staged ones were added and none were removed.
[ "${FILES_AFTER}" -gt "${FILES_BEFORE}" ] && echo "staged files kept" || echo "FILES DELETED: ${FILES_BEFORE} -> ${FILES_AFTER}"
# Local storage publishes the conditional write before the failpoint fires, so the commit really did
# take effect and its rows are readable. What is asserted is the table surviving the throw: the
# reported failure must not have deleted anything the current snapshot references.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_UNKNOWN}"

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

for t in "${TABLE_INSERT}" "${TABLE_DELETE}" "${TABLE_UNKNOWN}" "${TABLE_CONTROL}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${t}"
done
