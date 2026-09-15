#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Position delete files are matched to data files by the partition spec of their manifests and the
# partition value of the entries (`defineDeletesSpan`). When a manifest header binds a schema-id to a
# schema that another header binds differently, and metadata.json does not define that id, the
# partition key expression of that manifest cannot be built under
# `iceberg_tolerate_conflicting_manifest_schemas`. The partition spec itself does not depend on the
# schema and must be kept, otherwise the entries of such a manifest carry an empty spec, no delete
# file matches them, and deleted rows come back. Here both data manifests are made unsettled while
# the delete manifest (processed first) is not, so the data files would lose their deletes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_unsettled_deletes"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

TABLE="t_${CLICKHOUSE_DATABASE}_unsettled_deletes"
TABLE_PATH="${ROOT}/${TABLE}/"

# Background Iceberg compaction would rewrite the very manifests this test patches, so it is pinned
# off per table. Each INSERT writes one partition, so one data manifest per INSERT.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "
    CREATE TABLE ${TABLE} (p Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (p)
    SETTINGS allow_experimental_iceberg_compaction = 0;
    INSERT INTO ${TABLE} VALUES (1, 10), (1, 11);
    INSERT INTO ${TABLE} VALUES (2, 20), (2, 21);
"

# The manifests written so far are the data manifests; the DELETE below adds a delete manifest.
DATA_MANIFESTS=()
for f in "${TABLE_PATH}"metadata/*.avro; do
    case "$(basename "$f")" in snap-*) continue;; esac
    DATA_MANIFESTS+=("$f")
done
[ "${#DATA_MANIFESTS[@]}" -eq 2 ] || { echo "expected two data manifest files, got ${#DATA_MANIFESTS[@]}"; exit 1; }

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --query "DELETE FROM ${TABLE} WHERE v IN (11, 21)"

echo "after delete"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT p, v FROM ${TABLE} ORDER BY p, v"

# Rewrites one value of the header metadata map of an Avro object-container file, keeping the sync
# marker and every data block verbatim. Only the header key `schema` is changed: the copy of
# schema-id 0 gets a different type for the column `v` than the delete manifest header carries.
HELPER="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_unsettled_deletes_avro_header.py"
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

for path in sys.argv[1:]:
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
python3 "${HELPER}" "${DATA_MANIFESTS[@]}"

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

# A second table object over the same files starts with an empty shared schema processor. The
# delete manifest is read first and registers schema-id 0 from its header; each data manifest header
# then conflicts with it, so both data manifests have an unsettled schema-id.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    CREATE TABLE ${TABLE}_read ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS allow_experimental_iceberg_compaction = 0
"

echo "strict read"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --iceberg_tolerate_conflicting_manifest_schemas=0 \
    --query "SELECT p, v FROM ${TABLE}_read ORDER BY p, v" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1

# The rows are read with the schema metadata.json assigns to their snapshots, and the position
# deletes still apply: the data manifests keep their partition spec, so they match the delete files.
echo "tolerant read"
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --iceberg_tolerate_conflicting_manifest_schemas=1 --send_logs_level=error --query "
    SELECT p, v FROM ${TABLE}_read ORDER BY p, v;
    SELECT p, v FROM ${TABLE}_read WHERE p = 2 ORDER BY v;
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}_read SYNC; DROP TABLE IF EXISTS ${TABLE} SYNC"
