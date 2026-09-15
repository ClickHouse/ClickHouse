#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Two manifest file headers bind the same schema-id to different schemas, and metadata.json does
# not define that id at all (an expired schema still referenced by old manifests). Neither header
# copy is authoritative, so nothing may be derived from that schema-id until metadata.json settles
# it. A walk that only collects file paths and record counts, such as `remove_orphan_files`, must
# still get through such manifests under `iceberg_tolerate_conflicting_manifest_schemas`: it never
# transforms rows with the ambiguous schema. The partition key of a manifest with an unsettled
# schema-id is simply not built. The strict mode fails already at the second header, as before.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_unsettled"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

TABLE="t_${CLICKHOUSE_DATABASE}_unsettled"
TABLE_PATH="${ROOT}/${TABLE}/"

# Background Iceberg compaction would rewrite the very manifests this test patches, so it is pinned
# off per table. Each INSERT writes one partition, so one manifest per INSERT, both under schema-id 0.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "
    CREATE TABLE ${TABLE} (p Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (p)
    SETTINGS allow_experimental_iceberg_compaction = 0;
    INSERT INTO ${TABLE} VALUES (1, 10), (1, 11);
    INSERT INTO ${TABLE} VALUES (2, 20), (2, 21);
"

# Rewrites one value of the header metadata map of an Avro object-container file, keeping the sync
# marker and every data block verbatim. Only the header key `schema` of one manifest is changed:
# its copy of schema-id 0 gets a different type for the column `v` than the other manifest carries.
HELPER="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_unsettled_avro_header.py"
cat > "${HELPER}" <<'PY'
import json
import sys

MAGIC = b"Obj\x01"

def read_long(b, p):
    shift = acc = 0
    while True:
        c = b[p]; p += 1
        acc |= (c & 0x7F) << shift
        if not c & 0x80:
            return (acc >> 1) ^ -(acc & 1), p
        shift += 7

def write_long(n):
    z = (n << 1) if n >= 0 else ((-n) << 1) - 1
    out = bytearray()
    while True:
        c = z & 0x7F; z >>= 7
        out.append(c | 0x80 if z else c)
        if not z:
            return bytes(out)

def read_bytes(b, p):
    n, p = read_long(b, p)
    return b[p:p + n], p + n

path = sys.argv[1]
buf = open(path, "rb").read()
assert buf[:4] == MAGIC, "not an Avro object container file"

pos, items = 4, []
while True:
    count, pos = read_long(buf, pos)
    if count == 0:
        break
    if count < 0:
        count = -count
        _, pos = read_long(buf, pos)
    for _ in range(count):
        k, pos = read_bytes(buf, pos)
        v, pos = read_bytes(buf, pos)
        items.append((k, v))

out = bytearray(MAGIC) + write_long(len(items))
patched = False
for k, v in items:
    if k == b"schema":
        schema = json.loads(v)
        for field in schema["fields"]:
            if field["name"] == "v":
                assert field["type"] == "int", field
                field["type"] = "long"
                patched = True
        v = json.dumps(schema).encode()
    out += write_long(len(k)) + k + write_long(len(v)) + v
assert patched, "column v not found in the manifest header schema"
out += write_long(0) + buf[pos:]
open(path, "wb").write(bytes(out))
PY

MANIFESTS=()
for f in "${TABLE_PATH}"metadata/*.avro; do
    case "$(basename "$f")" in snap-*) continue;; esac
    MANIFESTS+=("$f")
done
[ "${#MANIFESTS[@]}" -eq 2 ] || { echo "expected two manifest files, got ${#MANIFESTS[@]}"; exit 1; }
python3 "${HELPER}" "${MANIFESTS[1]}"

# Renumber the only schema of metadata.json to schema-id 1, so that schema-id 0 of the manifest
# headers is not defined by metadata.json anymore.
LATEST_METADATA=$(ls "${TABLE_PATH}"metadata/v*.metadata.json | sed 's#.*/v##;s#\.metadata.json##' | sort -n | tail -1)
python3 - "${TABLE_PATH}metadata/v${LATEST_METADATA}.metadata.json" <<'PY'
import json
import sys

path = sys.argv[1]
meta = json.load(open(path))
assert [schema["schema-id"] for schema in meta["schemas"]] == [0], meta["schemas"]
meta["schemas"][0]["schema-id"] = 1
meta["current-schema-id"] = 1
for snapshot in meta["snapshots"]:
    snapshot["schema-id"] = 1
if "schema" in meta:
    meta["schema"]["schema-id"] = 1
json.dump(meta, open(path, "w"))
PY

# A second table object over the same files starts with an empty shared schema processor, so the
# manifest walk reaches both header copies of schema-id 0 without any metadata.json copy of it.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    CREATE TABLE ${TABLE}_walk ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS allow_experimental_iceberg_compaction = 0
"

echo "strict remove_orphan_files"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --allow_iceberg_remove_orphan_files=1 --use_iceberg_metadata_files_cache=0 \
    --iceberg_tolerate_conflicting_manifest_schemas=0 \
    --query "ALTER TABLE ${TABLE}_walk EXECUTE remove_orphan_files()" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1

echo "tolerant remove_orphan_files"
# Every file is younger than the orphan age window, so the walk deletes nothing; what matters is
# that it gets through both manifests and reports.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --allow_iceberg_remove_orphan_files=1 --use_iceberg_metadata_files_cache=0 \
    --iceberg_tolerate_conflicting_manifest_schemas=1 --send_logs_level=error \
    --query "ALTER TABLE ${TABLE}_walk EXECUTE remove_orphan_files()"

# The rows are still read with the schema metadata.json assigns to their snapshots. The manifest
# whose schema-id stayed unsettled is not pruned by partition, which only costs, never loses, rows.
echo "tolerant read"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --iceberg_tolerate_conflicting_manifest_schemas=1 --send_logs_level=error --query "
    SELECT count(), sum(v) FROM ${TABLE}_walk;
    SELECT p, v FROM ${TABLE}_walk WHERE p = 1 ORDER BY v;
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}_walk SYNC; DROP TABLE IF EXISTS ${TABLE} SYNC"
