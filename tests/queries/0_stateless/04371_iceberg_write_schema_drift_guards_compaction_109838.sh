#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, long
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# - long: several full Iceberg table-lifecycle scenarios; exempts the 180s flaky-check cap.
# - no-parallel: the drift scenarios rely on the server-global Iceberg metadata cache staying
#   warm (or being dropped) at a precise point; a concurrent SYSTEM DROP ICEBERG METADATA CACHE
#   or LRU eviction would refresh the cached schema and the drift would stop firing.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Part 3/5 of the Iceberg write/mutation/compaction schema-drift guard regression
# (issues #109835 / #109838): OPTIMIZE compaction (Compaction.cpp) - schema/field-id drift.
# Part 1 (INSERT sink) lives in 04365_iceberg_write_schema_drift_guards_insert_109838.sh.
# Part 2 (UPDATE/DELETE mutation) lives in 04369_iceberg_write_schema_drift_guards_mutation_109838.sh.
# Part 4 (OPTIMIZE compaction - spec/leak/evolution) lives in
# 04372_iceberg_write_schema_drift_guards_compaction_spec_109838.sh.
# Part 5 (metadata edge cases) lives in 04373_iceberg_write_schema_drift_guards_metadata_109838.sh.
# The Iceberg write paths map input block columns positionally onto schema fields, so a stale
# attached table or malformed metadata could abort the server (field_ids[] out of bounds) or
# silently commit data files with the wrong names/types/field-ids. Each scenario asserts a clean
# query error (not an abort) and that the server stays alive.
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
# OPTIMIZE compaction (Compaction.cpp): schema/field-id drift
# ============================================================================================

# --- OPTIMIZE same-width MODIFY drift (c0 Int32 -> Int64): rejected ---------------------------
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "
CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2, allow_experimental_iceberg_compaction=1;
INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3);
ALTER TABLE ${TABLE} DELETE WHERE c0 = 0;
"
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
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --allow_experimental_iceberg_compaction=1 --query "
CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2, allow_experimental_iceberg_compaction=1;
INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3);
ALTER TABLE ${TABLE} DELETE WHERE c0 = 0;
ALTER TABLE ${TABLE} DROP COLUMN c0;
ALTER TABLE ${TABLE} ADD COLUMN c0 Nullable(Int32);
OPTIMIZE TABLE ${TABLE};
" 2>&1 | grep -oF "BAD_ARGUMENTS" | head -1

# --- OPTIMIZE on a malformed table (current-schema-id resolves to no schema): rejected --------
# getPlan builds ManifestFilePlan(current_schema) -> DataFileStatistics derefs schema_->size()
# before the validate helper runs, so the null-schema guard must sit ahead of getPlan.
reset
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "
CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version=2, allow_experimental_iceberg_compaction=1;
INSERT INTO ${TABLE} SELECT number, 'x' FROM numbers(3);
ALTER TABLE ${TABLE} DELETE WHERE c0 = 0;
"
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

# The server must still be alive after all scenarios (no abort).
${CLICKHOUSE_CLIENT} --query "SELECT 1"

reset
