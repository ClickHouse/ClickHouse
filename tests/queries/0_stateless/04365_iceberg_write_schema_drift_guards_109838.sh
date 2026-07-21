#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, long
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# - long: many full Iceberg table-lifecycle scenarios; exempts the 180s flaky-check cap.
# - no-parallel: the drift scenarios rely on the server-global Iceberg metadata cache staying
#   warm (or being dropped) at a precise point; a concurrent SYSTEM DROP ICEBERG METADATA CACHE
#   or LRU eviction would refresh the cached schema and the drift would stop firing.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Part 1/2 of the Iceberg write/mutation/compaction schema-drift guard regression
# (issues #109835 / #109838): INSERT sink, UPDATE/DELETE mutation, OPTIMIZE compaction.
# Part 2 lives in 04368_iceberg_write_schema_drift_guards_metadata_109838.sh.
# The Iceberg write paths map input block columns positionally onto schema fields, so a
# stale attached table or malformed metadata could abort the server (field_ids[] out of
# bounds) or silently commit data files with the wrong names/types/field-ids. Each scenario
# asserts a clean query error (not an abort) and that the server stays alive.
# Each scenario gets a fresh table name+path (see reset), removing cross-scenario cache coupling.

_scenario=0
TABLE="t_${CLICKHOUSE_DATABASE}_0"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

reset() {
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
    _scenario=$((_scenario + 1))
    TABLE="t_${CLICKHOUSE_DATABASE}_${_scenario}"
    TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
}

# Publish a new metadata version derived from the latest one, applying a python edit passed on stdin.
# The edit receives the parsed dict `m` and must leave the new version in `m`.
publish_next_metadata() {
    python3 - "${TABLE_PATH}/metadata" "$1"
}

# ============================================================================================
# INSERT sink
# ============================================================================================
# These two scenarios run in clickhouse-local, not against the shared test server: on unpatched
# master the column-count-mismatch INSERT aborts the process (getColumnSizes indexes field_ids[]
# out of bounds). Against clickhouse-client that abort lands in the long-running server, the
# hung-check terminates the runner before a FAIL is recorded, and Bugfix validation cannot invert
# it to OK (amd64 tolerates it, aarch64 reports "server died"). clickhouse-local contains the
# abort to a short-lived subprocess: the runner sees a non-zero exit + empty stdout, diffs it
# against .reference, and reports a normal FAIL. The sibling table on the same path is a second
# IcebergLocal attachment (a different table name), matching the server scenario; clickhouse-local
# only rejects a second attachment under the SAME name, so distinct names work here.

# --- INSERT column-count mismatch (sibling DROP COLUMN): rejected, no abort ------------------
INSERT_DIR="${CLICKHOUSE_TMP}/04365_insert_dropcol_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${INSERT_DIR}"; mkdir -p "${INSERT_DIR}/t"
${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0 --multiquery -q "
CREATE TABLE src (c0 Int64, c1 Int64, c2 Int64) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t (c0 Int64, c1 Int64, c2 Int64) ENGINE = IcebergLocal('${INSERT_DIR}/t/', 'Avro');
INSERT INTO t VALUES (0, 0, 0);
CREATE MATERIALIZED VIEW mv TO t AS SELECT c0, c1, c2 FROM src;
CREATE TABLE IF NOT EXISTS tsib (c0 Int64, c1 Int64, c2 Int64) ENGINE = IcebergLocal('${INSERT_DIR}/t/', 'Avro');
ALTER TABLE tsib DROP COLUMN c2;
INSERT INTO src VALUES (1, 2, 3);
" -- --user_files_path="${INSERT_DIR}" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1
rm -rf "${INSERT_DIR}"

# --- INSERT same-width RENAME drift: rejected by the full (names+types) check -----------------
RENAME_DIR="${CLICKHOUSE_TMP}/04365_insert_rename_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${RENAME_DIR}"; mkdir -p "${RENAME_DIR}/t"
${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0 --multiquery -q "
CREATE TABLE src (c0 Int64, c1 Int64) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t (c0 Int64, c1 Int64) ENGINE = IcebergLocal('${RENAME_DIR}/t/', 'Avro');
INSERT INTO t VALUES (0, 0);
CREATE MATERIALIZED VIEW mv TO t AS SELECT c0, c1 FROM src;
CREATE TABLE IF NOT EXISTS tsib (c0 Int64, c1 Int64) ENGINE = IcebergLocal('${RENAME_DIR}/t/', 'Avro');
ALTER TABLE tsib RENAME COLUMN c1 TO c1_renamed;
INSERT INTO src VALUES (1, 2);
" -- --user_files_path="${RENAME_DIR}" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1
rm -rf "${RENAME_DIR}"

# ============================================================================================
# UPDATE mutation (Mutations.cpp)
# ============================================================================================
# Mutation tables use Parquet: Iceberg UPDATE/DELETE are only supported for Parquet data files.

# --- UPDATE same-width rename drift: rejected -------------------------------------------------
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3)"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
publish_next_metadata rename_c0_to_c9_new_schema <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
ns = json.loads(json.dumps(m['schemas'][0])); ns['schema-id'] = 1
for f in ns['fields']:
    if f['name'] == 'c0':
        f['name'] = 'c9'
m['schemas'].append(ns)
m['current-schema-id'] = 1
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=600000 \
    --query "ALTER TABLE ${TABLE} UPDATE c1 = 'y' WHERE c0 = 1" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1

# --- DELETE same-width rename drift: rejected ------------------------------------------------
# A partitioned DELETE builds ChunkPartitioner from the current schema against the stale header.
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (c0) SETTINGS iceberg_format_version=2"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3)"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
publish_next_metadata rename_c1_to_c9_new_schema <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
ns = json.loads(json.dumps(m['schemas'][0])); ns['schema-id'] = 1
for f in ns['fields']:
    if f['name'] == 'c1':
        f['name'] = 'c9'
m['schemas'].append(ns)
m['current-schema-id'] = 1
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=600000 \
    --query "ALTER TABLE ${TABLE} DELETE WHERE c0 = 1" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1

# --- UPDATE on a malformed table (current-schema-id resolves to no schema): rejected ----------
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3)"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
publish_next_metadata current_schema_id_absent <<'PY'
import json, os, sys
md = sys.argv[1]
m = json.load(open(os.path.join(md, 'v2.metadata.json')))
m['current-schema-id'] = 999
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_v3'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, 'v3.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=600000 \
    --query "ALTER TABLE ${TABLE} UPDATE c1 = 'y' WHERE c0 = 1" 2>&1 | grep -oF "ICEBERG_SPECIFICATION_VIOLATION" | head -1

# --- UPDATE without drift: still works --------------------------------------------------------
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} UPDATE c1 = 'y' WHERE c0 = 1"
${CLICKHOUSE_CLIENT} --query "SELECT c0, c1 FROM ${TABLE} ORDER BY c0"

# --- UPDATE across a same-layout partition-spec rebind: allowed ------------------------------
# Another engine may republish the identical partition layout under a new spec-id. The mutation
# guard must reject only a genuine layout change, not the id alone (Compaction.cpp compares the
# layout field-by-field). Data files stay tagged spec-id 0; default becomes spec-id 1 with the
# same fields, so position deletes still land in the right partitions. The UPDATE must succeed
# and supersede the old row.
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (c1) SETTINGS iceberg_format_version=2"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'a')"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
publish_next_metadata rebind_same_layout_new_spec_id <<'PY'
import json, os, sys, glob, re
md = sys.argv[1]
vs = sorted(glob.glob(os.path.join(md, 'v*.metadata.json')),
            key=lambda p: int(re.search(r'v(\d+)', os.path.basename(p)).group(1)))
latest = vs[-1]
m = json.load(open(latest))
# Duplicate the current default spec's layout under a new spec-id, then make it the default.
spec0 = [s for s in m['partition-specs'] if s['spec-id'] == m['default-spec-id']][0]
new = json.loads(json.dumps(spec0)); new['spec-id'] = 1
m['partition-specs'].append(new)
m['default-spec-id'] = 1
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
next_n = int(re.search(r'v(\d+)', os.path.basename(latest)).group(1)) + 1
tmp = os.path.join(md, '.tmp_next'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, f'v{next_n}.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=600000 \
    --query "ALTER TABLE ${TABLE} UPDATE c0 = 99 WHERE c1 = 'b'"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ICEBERG METADATA CACHE"
${CLICKHOUSE_CLIENT} --query "SELECT c0, c1 FROM ${TABLE} ORDER BY c1, c0"

# ============================================================================================
# OPTIMIZE compaction (Compaction.cpp)
# ============================================================================================

# --- OPTIMIZE same-width MODIFY drift (c0 Int32 -> Int64): rejected ---------------------------
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2, allow_experimental_iceberg_compaction=1"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE c0 = 0"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
publish_next_metadata modify_c0_to_long_new_schema <<'PY'
import json, os, sys, glob
md = sys.argv[1]
vs = sorted(glob.glob(os.path.join(md, 'v*.metadata.json')),
            key=lambda p: int(''.join(filter(str.isdigit, os.path.basename(p).split('.')[0]))))
latest = vs[-1]
m = json.load(open(latest))
max_sid = max(s['schema-id'] for s in m['schemas'])
cur = [s for s in m['schemas'] if s['schema-id'] == m['current-schema-id']][0]
ns = json.loads(json.dumps(cur)); ns['schema-id'] = max_sid + 1
for f in ns['fields']:
    if f['name'] == 'c0':
        f['type'] = 'long'
m['schemas'].append(ns)
m['current-schema-id'] = max_sid + 1
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
next_n = int(''.join(filter(str.isdigit, os.path.basename(latest).split('.')[0]))) + 1
tmp = os.path.join(md, '.tmp_next'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, f'v{next_n}.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --iceberg_metadata_staleness_ms=600000 \
    --query "OPTIMIZE TABLE ${TABLE}" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1

# --- OPTIMIZE DROP c0 + ADD c0 field-id reuse: rejected ---------------------------------------
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2, allow_experimental_iceberg_compaction=1"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE c0 = 0"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP COLUMN c0"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} ADD COLUMN c0 Nullable(Int32)"
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1

# --- OPTIMIZE on a malformed table (current-schema-id resolves to no schema): rejected --------
# getPlan builds ManifestFilePlan(current_schema) -> DataFileStatistics derefs schema_->size()
# before the validate helper runs, so the null-schema guard must sit ahead of getPlan.
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2, allow_experimental_iceberg_compaction=1"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE c0 = 0"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
publish_next_metadata optimize_current_schema_id_absent <<'PY'
import json, os, sys, glob
md = sys.argv[1]
vs = sorted(glob.glob(os.path.join(md, 'v*.metadata.json')),
            key=lambda p: int(''.join(filter(str.isdigit, os.path.basename(p).split('.')[0]))))
latest = vs[-1]
m = json.load(open(latest))
m['current-schema-id'] = 999
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
next_n = int(''.join(filter(str.isdigit, os.path.basename(latest).split('.')[0]))) + 1
tmp = os.path.join(md, '.tmp_next'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, f'v{next_n}.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --iceberg_metadata_staleness_ms=600000 \
    --query "OPTIMIZE TABLE ${TABLE}" 2>&1 | grep -oF "ICEBERG_SPECIFICATION_VIOLATION" | head -1

# --- OPTIMIZE on a table whose default-spec-id resolves to no partition-specs entry: rejected -
# The default-spec-id check runs before writeDataFiles (which has no cleanup), so the rejection
# must not leave orphaned patched data files. Assert the data-file count is unchanged.
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2, allow_experimental_iceberg_compaction=1"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE c0 = 0"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
data_files_before=$(find "${TABLE_PATH}/data" -name '*.parquet' 2>/dev/null | wc -l)
publish_next_metadata optimize_default_spec_id_absent <<'PY'
import json, os, sys, glob
md = sys.argv[1]
vs = sorted(glob.glob(os.path.join(md, 'v*.metadata.json')),
            key=lambda p: int(''.join(filter(str.isdigit, os.path.basename(p).split('.')[0]))))
latest = vs[-1]
m = json.load(open(latest))
m['default-spec-id'] = 999
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
next_n = int(''.join(filter(str.isdigit, os.path.basename(latest).split('.')[0]))) + 1
tmp = os.path.join(md, '.tmp_next'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, f'v{next_n}.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --iceberg_metadata_staleness_ms=600000 \
    --query "OPTIMIZE TABLE ${TABLE}" 2>&1 | grep -oF "ICEBERG_SPECIFICATION_VIOLATION" | head -1
data_files_after=$(find "${TABLE_PATH}/data" -name '*.parquet' 2>/dev/null | wc -l)
[ "${data_files_before}" -eq "${data_files_after}" ] && echo "NO_LEAK" || echo "LEAKED_PATCHED_FILES"

# --- OPTIMIZE across a partition-spec evolution: rejected -------------------------------------
# The data file was written under the original partition spec, but a sibling engine evolved the
# default partition spec (new spec-id, different columns). writeMetadataFiles re-serializes every
# rewritten file under the CURRENT default spec only, so the file's partition values would be
# reinterpreted under the wrong columns/transforms (wrong metadata / out-of-bounds if arity grew).
# The per-file spec check must reject this before writeDataFiles runs.
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (c1) SETTINGS iceberg_format_version=2, allow_experimental_iceberg_compaction=1"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'a')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE c0 = 2"
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=600000 --query "SELECT count() FROM ${TABLE}" > /dev/null
publish_next_metadata optimize_partition_spec_evolution <<'PY'
import json, os, sys, glob, re
md = sys.argv[1]
vs = sorted(glob.glob(os.path.join(md, 'v*.metadata.json')),
            key=lambda p: int(re.search(r'v(\d+)', os.path.basename(p)).group(1)))
latest = vs[-1]
m = json.load(open(latest))
# Evolve the default partition spec: a new spec (spec-id 1) partitioning by c0 instead of c1.
m['partition-specs'].append({'spec-id': 1, 'fields': [
    {'field-id': 1002, 'name': 'c0', 'source-id': 1, 'transform': 'identity'}]})
m['default-spec-id'] = 1
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
next_n = int(re.search(r'v(\d+)', os.path.basename(latest)).group(1)) + 1
tmp = os.path.join(md, '.tmp_next'); json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, f'v{next_n}.metadata.json'))
PY
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --iceberg_metadata_staleness_ms=600000 \
    --query "OPTIMIZE TABLE ${TABLE}" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1

# --- OPTIMIZE without any evolution: still compacts -------------------------------------------
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2, allow_experimental_iceberg_compaction=1"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE c0 = 0"
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

# The server must still be alive after all scenarios (no abort).
${CLICKHOUSE_CLIENT} --query "SELECT 1"

reset
