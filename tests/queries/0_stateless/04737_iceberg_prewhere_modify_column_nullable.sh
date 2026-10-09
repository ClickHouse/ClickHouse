#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# `no-parallel-replicas`: see comment in `04071_iceberg_orc_prewhere_crash.sh`.
# `StorageObjectStorageCluster` (used when `parallel_replicas_for_cluster_engines = 1`,
# default) does not delegate `supportsPrewhere` to its underlying configuration.
#
# Regression test for issue #85029: filtering a column that `ALTER TABLE ... MODIFY COLUMN`
# made `Nullable` fails on the Iceberg data files written before the `ALTER`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `optimize_move_to_prewhere=1` + `query_plan_optimize_prewhere=1` are pinned on every
# discriminating statement: the failure only appears when the predicate is pushed into the
# reader, the runner randomizes both, and with either one off the pre-fix result is already
# correct, so the test would stop exercising the fix.
PREWHERE_SETTINGS="--optimize_move_to_prewhere=1 --query_plan_optimize_prewhere=1"

TABLE="t_null_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_REN="t_ren_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_WID="t_wid_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_REJ="t_rej_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_STR="t_str_${CLICKHOUSE_DATABASE}_${RANDOM}"

drop_table() {
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $1"
    rm -rf "${USER_FILES_PATH}/$1/"
}

# Rows 0..4 are written while `id` is still required, rows 5 and NULL after the `ALTER`, so the
# table mixes pre- and post-evolution data files. Only the pre-evolution ones carry the defect.
create_mixed_nullability_table() {
    local table="$1"
    local table_path="${USER_FILES_PATH}/${table}/"
    rm -rf "${table_path}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (id Int64, s String) ENGINE = IcebergLocal('${table_path}', 'Parquet')"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} SELECT number, toString(number) FROM numbers(5)"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${table} MODIFY COLUMN id Nullable(Int64)"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} SELECT * FROM values('id Nullable(Int64), s String', (5, 'five'), (NULL, 'none'))"
}

create_mixed_nullability_table "${TABLE}"

echo "--- WHERE on the evolved column ---"
${CLICKHOUSE_CLIENT} ${PREWHERE_SETTINGS} --query "SELECT id, s FROM ${TABLE} WHERE id > 3 ORDER BY id"

# Explicit PREWHERE probes the reader-side path directly, without depending on the
# WHERE->PREWHERE mover.
echo "--- PREWHERE on the evolved column ---"
${CLICKHOUSE_CLIENT} ${PREWHERE_SETTINGS} --query "SELECT id, s FROM ${TABLE} PREWHERE id > 3 ORDER BY id"

echo "--- PREWHERE IS NULL / IS NOT NULL ---"
${CLICKHOUSE_CLIENT} ${PREWHERE_SETTINGS} --query "SELECT s FROM ${TABLE} PREWHERE id IS NULL ORDER BY s"
${CLICKHOUSE_CLIENT} ${PREWHERE_SETTINGS} --query "SELECT count() FROM ${TABLE} PREWHERE id IS NOT NULL"

echo "--- PREWHERE on an untouched column ---"
${CLICKHOUSE_CLIENT} ${PREWHERE_SETTINGS} --query "SELECT id, s FROM ${TABLE} PREWHERE s = '4' ORDER BY id"

echo "--- declared type and full scan ---"
${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(id > 3), toTypeName(any(id)) FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --query "SELECT id, s FROM ${TABLE} ORDER BY id, s"

# Composition with a rename: the transform must apply the new name and the new nullability in one
# node. Before the fix the rename branch renamed the column and dropped the type change.
echo "--- MODIFY COLUMN to Nullable plus RENAME COLUMN ---"
create_mixed_nullability_table "${TABLE_REN}"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE_REN} RENAME COLUMN id TO idx"
${CLICKHOUSE_CLIENT} ${PREWHERE_SETTINGS} --query "SELECT idx, s, toTypeName(idx) FROM ${TABLE_REN} PREWHERE idx > 3 ORDER BY idx"

# Widening and nullability at once already took the type-conversion branch and was already
# correct; assert it stays correct.
echo "--- widening plus nullability (int required to long optional) ---"
rm -rf "${USER_FILES_PATH}/${TABLE_WID}/"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE_WID} (id Int32, s String) ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_WID}/', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE_WID} SELECT toInt32(number), toString(number) FROM numbers(5)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE_WID} MODIFY COLUMN id Nullable(Int64)"
${CLICKHOUSE_CLIENT} ${PREWHERE_SETTINGS} --query "SELECT id, toTypeName(id) FROM ${TABLE_WID} PREWHERE id > 3 ORDER BY id"

# The reverse direction is not legal evolution and must keep being rejected.
echo "--- optional to required is still rejected ---"
rm -rf "${USER_FILES_PATH}/${TABLE_REJ}/"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE_REJ} (id Nullable(Int64), s String) ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_REJ}/', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE_REJ} SELECT number, toString(number) FROM numbers(3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE_REJ} MODIFY COLUMN id Int64" 2>&1 \
    | grep -oF "Iceberg spec doesn't allow change type from nullable to non-nullable" | head -1

# A non-numeric type reaches the same branch through a different comparison function.
echo "--- String column made Nullable ---"
rm -rf "${USER_FILES_PATH}/${TABLE_STR}/"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE_STR} (v String, s String) ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE_STR}/', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE_STR} SELECT toString(number), toString(number) FROM numbers(5)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE_STR} MODIFY COLUMN v Nullable(String)"
${CLICKHOUSE_CLIENT} ${PREWHERE_SETTINGS} --query "SELECT v, toTypeName(v) FROM ${TABLE_STR} PREWHERE v = '4' ORDER BY v"

# The reverse direction (optional -> required) cannot be produced by `ALTER`, which rejects it
# above, so the passthrough branch is reached only through metadata written by another engine.
# Appending a schema leaves schema 0 byte-identical, so the schema-id immutability check still
# passes, and the read selects the pair with `iceberg_metadata_file_path`.
echo "--- externally authored optional to required stays a passthrough ---"
TABLE_REV="t_rev_${CLICKHOUSE_DATABASE}_${RANDOM}"
REV_PATH="${USER_FILES_PATH}/${TABLE_REV}/"
rm -rf "${REV_PATH}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE_REV} (id Nullable(Int64), s String) ENGINE = IcebergLocal('${REV_PATH}', 'Parquet')"
# The NULL row is what makes the assertion non-vacuous: a cast to the required type only
# misbehaves when a NULL actually has to pass through it.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE_REV} SELECT * FROM values('id Nullable(Int64), s String', (1, 'one'), (NULL, 'none'), (3, 'three'))"

LATEST_REV=$(ls "${REV_PATH}metadata/" | grep -E '^v[0-9]+\.metadata\.json$' | sort -t v -k2 -n | tail -1)
REV_META=$(python3 - "${REV_PATH}metadata" "${LATEST_REV}" <<'PYEOF'
import copy, json, os, re, sys

metadata_dir, latest_file = sys.argv[1], sys.argv[2]

with open(os.path.join(metadata_dir, latest_file)) as fh:
    metadata = json.load(fh)

current = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
tightened = copy.deepcopy(current)
tightened["schema-id"] = max(s["schema-id"] for s in metadata["schemas"]) + 1
for field in tightened["fields"]:
    if field["name"] == "id":
        field["required"] = True

metadata["schemas"].append(tightened)
metadata["current-schema-id"] = tightened["schema-id"]
metadata["last-updated-ms"] = metadata.get("last-updated-ms", 0) + 60000

version = int(re.match(r"v(\d+)\.metadata\.json", latest_file).group(1)) + 1
tmp_file = os.path.join(metadata_dir, ".tmp_next")
with open(tmp_file, "w") as fh:
    json.dump(metadata, fh)
os.rename(tmp_file, os.path.join(metadata_dir, f"v{version}.metadata.json"))
print(f"metadata/v{version}.metadata.json")
PYEOF
)

REV_TF="icebergLocal('${REV_PATH}', 'Parquet', SETTINGS iceberg_metadata_file_path = '${REV_META}')"
# The tightened schema is what the reader resolves against.
${CLICKHOUSE_CLIENT} --query "DESCRIBE ${REV_TF}" | cut -f1,2
# Each of these three shapes fails if the transform casts the old optional column to the new
# required type instead of passing it through.
${CLICKHOUSE_CLIENT} --query "SELECT s FROM ${REV_TF} ORDER BY s"
${CLICKHOUSE_CLIENT} --query "SELECT s FROM ${REV_TF} WHERE id IS NOT NULL ORDER BY s"
${CLICKHOUSE_CLIENT} ${PREWHERE_SETTINGS} --query "SELECT s FROM ${REV_TF} PREWHERE id > 1 ORDER BY s"

drop_table "${TABLE}"
drop_table "${TABLE_REN}"
drop_table "${TABLE_WID}"
drop_table "${TABLE_REJ}"
drop_table "${TABLE_STR}"
drop_table "${TABLE_REV}"
