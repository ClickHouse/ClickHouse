#!/usr/bin/env bash
# Tags: no-fasttest

# `OPTIMIZE TABLE` on an Iceberg table commits one `replace` snapshot on top of the current one: earlier snapshots,
# table-level metadata and files stay as they were. https://github.com/ClickHouse/ClickHouse/issues/123982

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The cloud build does not run this compaction path.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
if [[ "${IS_CLOUD}" = "1" ]]; then
    cat "${CUR_DIR}/$(basename "${BASH_SOURCE[0]}" .sh).reference"
    exit 0
fi

BASE="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05325"
rm -rf "${BASE}"

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --use_iceberg_metadata_files_cache=0"
OPTIMIZE="${CH} --allow_experimental_iceberg_compaction=1"

latest_version() { ls "$1/metadata" | grep -oE '^v[0-9]+\.metadata\.json$' | grep -oE '[0-9]+' | sort -n | tail -1; }
latest_metadata() { echo "$1/metadata/v$(latest_version "$1").metadata.json"; }
list_files() { (cd "$1" && find . -type f | sort); }
metadata_field() { ${CH} --query "SELECT JSONExtractRaw(json, '$2') FROM file('$(latest_metadata "$1")', JSONAsString)"; }

live_files() {
    ${CH} --query "
        SELECT countIf(content = 'DATA'), countIf(content = 'POSITION_DELETE')
        FROM system.iceberg_files WHERE database = currentDatabase() AND table = '$1'"
}

# Prints the operation of the current snapshot and whether its parent is `$3`.
current_snapshot() {
    local current
    current=$(metadata_field "$1" current-snapshot-id)
    ${CH} --query "
        SELECT operation, parent_id = $3, summary['total-records'], summary['total-position-deletes']
        FROM system.iceberg_history
        WHERE database = currentDatabase() AND table = '$2' AND snapshot_id = ${current}"
}

echo '--- partitioned table'
T1="${BASE}/t1"
${CH} --query "CREATE TABLE t1 (x Int64, p Int64) ENGINE = IcebergLocal('${T1}/', 'Parquet') PARTITION BY p"
${CH} --query "INSERT INTO t1 VALUES (1, 0), (2, 1), (3, 0), (4, 1)"
S_INSERT=$(metadata_field "${T1}" current-snapshot-id)
UUID=$(metadata_field "${T1}" table-uuid | tr -d '"')
${CH} --query "CREATE TABLE t1_by_uuid ENGINE = IcebergLocal('${T1}/', 'Parquet') SETTINGS iceberg_metadata_table_uuid = '${UUID}'"
${CH} --query "ALTER TABLE t1 DELETE WHERE x = 1"
S_DELETE=$(metadata_field "${T1}" current-snapshot-id)
VERSION_BEFORE=$(latest_version "${T1}")
TABLE_FIELDS_BEFORE="$(metadata_field "${T1}" table-uuid) $(metadata_field "${T1}" partition-specs) $(metadata_field "${T1}" properties)"
FILES_BEFORE=$(list_files "${T1}")
TIME_TRAVEL_BEFORE=$(${CH} --query "SELECT arraySort(groupArray(x)) FROM t1 SETTINGS iceberg_snapshot_id = ${S_INSERT}")

${OPTIMIZE} --query "OPTIMIZE TABLE t1" 2>&1
echo "new versions: $(( $(latest_version "${T1}") - VERSION_BEFORE ))"
TABLE_FIELDS_AFTER="$(metadata_field "${T1}" table-uuid) $(metadata_field "${T1}" partition-specs) $(metadata_field "${T1}" properties)"
echo "table fields unchanged: $([[ "${TABLE_FIELDS_BEFORE}" == "${TABLE_FIELDS_AFTER}" ]] && echo 1 || echo 0)"
echo "files removed: $(comm -23 <(echo "${FILES_BEFORE}") <(list_files "${T1}") | wc -l)"
current_snapshot "${T1}" t1 "${S_DELETE}"
live_files t1
${CH} --query "SELECT x, p FROM t1 ORDER BY x"
${CH} --query "SELECT count(), (SELECT count() FROM t1 SETTINGS optimize_trivial_count_query = 0) FROM t1"

echo '--- time travel'
echo "${TIME_TRAVEL_BEFORE}"
${CH} --query "SELECT arraySort(groupArray(x)) FROM t1 SETTINGS iceberg_snapshot_id = ${S_INSERT}"
${CH} --query "SELECT arraySort(groupArray(x)) FROM t1 SETTINGS iceberg_snapshot_id = ${S_DELETE}"

echo '--- pinned table uuid'
${CH} --query "SELECT arraySort(groupArray(x)) FROM t1_by_uuid"

echo '--- second round'
${CH} --query "INSERT INTO t1 VALUES (5, 0)"
${CH} --query "ALTER TABLE t1 DELETE WHERE x = 2"
${OPTIMIZE} --query "OPTIMIZE TABLE t1" 2>&1
${CH} --query "SELECT arraySort(groupArray(x)) FROM t1"
live_files t1

echo '--- after OPTIMIZE ... MANIFEST'
T2="${BASE}/t2"
${CH} --query "CREATE TABLE t2 (x Int64) ENGINE = IcebergLocal('${T2}/', 'Parquet')"
${CH} --query "INSERT INTO t2 VALUES (1), (2), (3)"
${CH} --query "INSERT INTO t2 VALUES (4), (5)"
${OPTIMIZE} --query "OPTIMIZE TABLE t2 MANIFEST SETTINGS iceberg_manifest_min_count_to_compact = 1" 2>&1
${CH} --query "ALTER TABLE t2 DELETE WHERE x = 1"
S_DELETE=$(metadata_field "${T2}" current-snapshot-id)
${OPTIMIZE} --query "OPTIMIZE TABLE t2" 2>&1
current_snapshot "${T2}" t2 "${S_DELETE}"
${CH} --query "SELECT arraySort(groupArray(x)) FROM t2"

echo '--- no position deletes'
VERSION_BEFORE=$(latest_version "${T2}")
${OPTIMIZE} --query "OPTIMIZE TABLE t2" 2>&1
echo "new versions: $(( $(latest_version "${T2}") - VERSION_BEFORE ))"

echo '--- null current-snapshot-id'
# The spec allows JSON null for "no current snapshot", and other writers emit it.
T5="${BASE}/t5"
${CH} --query "CREATE TABLE t5 (x Int64) ENGINE = IcebergLocal('${T5}/', 'Parquet')"
${CH} --query "INSERT INTO t5 VALUES (1), (2)"
${CH} --query "ALTER TABLE t5 DELETE WHERE x = 1"
python3 -c "import json, sys; p = sys.argv[1]; m = json.load(open(p)); m['current-snapshot-id'] = None; json.dump(m, open(p, 'w'))" "$(latest_metadata "${T5}")"
VERSION_BEFORE=$(latest_version "${T5}")
${OPTIMIZE} --query "OPTIMIZE TABLE t5" 2>&1
echo "new versions: $(( $(latest_version "${T5}") - VERSION_BEFORE ))"

echo '--- stale version hint'
T3="${BASE}/t3"
${CH} --query "CREATE TABLE t3 (x Int64) ENGINE = IcebergLocal('${T3}/', 'Parquet')"
${CH} --query "INSERT INTO t3 VALUES (1), (2)"
${CH} --query "ALTER TABLE t3 DELETE WHERE x = 1"
STALE_VERSION=$(latest_version "${T3}")
${CH} --query "INSERT INTO t3 VALUES (3)"
echo -n "${STALE_VERSION}" > "${T3}/metadata/version-hint.text"
${CH} --query "CREATE TABLE t3_hinted ENGINE = IcebergLocal('${T3}/', 'Parquet') SETTINGS iceberg_use_version_hint = 1"
${CH} --query "SELECT arraySort(groupArray(x)) FROM t3_hinted"
${OPTIMIZE} --query "OPTIMIZE TABLE t3_hinted" 2>&1
echo "hint names the latest version: $([[ "$(cat "${T3}/metadata/version-hint.text")" == "$(latest_version "${T3}")" ]] && echo 1 || echo 0)"
${CH} --query "SELECT arraySort(groupArray(x)) FROM t3_hinted"
${CH} --query "SELECT arraySort(groupArray(x)) FROM t3"

echo '--- schema evolution'
T4="${BASE}/t4"
${CH} --query "CREATE TABLE t4 (x Int32, s String) ENGINE = IcebergLocal('${T4}/', 'Parquet')"
${CH} --query "INSERT INTO t4 VALUES (1, 'a'), (2, 'b'), (3, 'c')"
${CH} --query "ALTER TABLE t4 RENAME COLUMN s TO t"
${CH} --query "ALTER TABLE t4 MODIFY COLUMN x Int64"
${CH} --query "ALTER TABLE t4 DELETE WHERE x = 1"
${OPTIMIZE} --query "OPTIMIZE TABLE t4" 2>&1
${CH} --query "SELECT x, t, toTypeName(x) FROM t4 ORDER BY x"

echo '--- expire_snapshots reclaims the replaced files'
${CH} --allow_experimental_expire_snapshots=1 --query "ALTER TABLE t4 EXECUTE expire_snapshots(retain_last = 1, retention_period = '1ms')" > /dev/null
echo "data files: $(ls "${T4}/data" | grep -c -v 'deletes'), delete files: $(ls "${T4}/data" | grep -c 'deletes')"
${CH} --query "SELECT x, t FROM t4 ORDER BY x"

echo '--- equality deletes are refused'
# The shared read-only fixture of 04613 (position and equality deletes; ClickHouse cannot write the latter).
# Parallel replicas cannot read deletes, so they are off for this table.
EQ="${CH} --enable_parallel_replicas=0"
${EQ} --query "CREATE TABLE t_eq ENGINE = IcebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table')"
eq_history() { ${EQ} --query "SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = 't_eq'"; }
HISTORY_BEFORE=$(eq_history)
ERR=$(${OPTIMIZE} --enable_parallel_replicas=0 --query "OPTIMIZE TABLE t_eq" 2>&1)
grep -oF NOT_IMPLEMENTED <<< "${ERR}" | head -n1
grep -oF 'equality deletes is not supported' <<< "${ERR}" | head -n1
echo "history unchanged: $([[ "$(eq_history)" == "${HISTORY_BEFORE}" ]] && echo 1 || echo 0)"
${EQ} --query "SELECT count() FROM t_eq"
${EQ} --query "DROP TABLE t_eq"

${CH} --query "DROP TABLE t1_by_uuid"
${CH} --query "DROP TABLE t3_hinted"
for t in t1 t2 t3 t4 t5; do ${CH} --query "DROP TABLE ${t}"; done
rm -rf "${BASE}"
