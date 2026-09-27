#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: needs the Parquet and DeltaLake readers.
# Tag no-msan: delta-kernel-rs is not built with MSan.

# A column added to a Delta table becomes visible through a Unity DataLakeCatalog database, also when the
# database sets `allow_experimental_iceberg_compaction`, which applies to Iceberg tables only.
# https://github.com/ClickHouse/ClickHouse/issues/122548

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/t"
STATE_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_columns.json"
PORT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_port"
DB="${CLICKHOUSE_DATABASE}_unity"
DATABASES="legacy legacy_compaction v2 v2_compaction"

cleanup() {
    for name in ${DATABASES}; do
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}_${name}"
    done
    [ -n "${HTTP_PID}" ] && kill "${HTTP_PID}" 2>/dev/null && wait "${HTTP_PID}" 2>/dev/null
    rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}" "${STATE_FILE}" "${PORT_FILE}"
}
trap cleanup EXIT

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}" "${PORT_FILE}"
mkdir -p "${TABLE_DIR}/_delta_log"
echo '[["a", "long"]]' > "${STATE_FILE}"

# A minimal Unity catalog serving the table `unity.default.t` with the columns listed in STATE_FILE.
python3 - "${STATE_FILE}" "${TABLE_DIR}" "${PORT_FILE}" <<'EOF' &
import json, os, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

state_file, table_dir, port_file = sys.argv[1:4]

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split('?')[0] == '/api/2.1/unity-catalog/tables/unity.default.t':
            with open(state_file) as f:
                columns = json.load(f)
            code, body = 200, {
                'name': 't', 'catalog_name': 'unity', 'schema_name': 'default',
                'table_type': 'EXTERNAL', 'data_source_format': 'DELTA',
                'storage_location': 'file://' + table_dir,
                'table_id': '11111111-2222-4333-8444-555555555555',
                'columns': [{'name': name, 'nullable': True,
                             'type_json': json.dumps({'name': name, 'type': type, 'nullable': True, 'metadata': {}})}
                            for name, type in columns]}
        else:
            code, body = 404, {'error_code': 'NOT_FOUND'}
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open(port_file + '.tmp', 'w') as f:
    f.write(str(server.server_address[1]))
os.rename(port_file + '.tmp', port_file)
server.serve_forever()
EOF
HTTP_PID=$!

for _ in $(seq 1 100); do
    [ -s "${PORT_FILE}" ] && break
    sleep 0.1
done
PORT=$(cat "${PORT_FILE}")

# write_commit <version> <columns as JSON> <data file>: one Delta log commit that sets the schema and adds a file.
write_commit() {
    python3 - "${TABLE_DIR}" "$1" "$2" "$3" <<'EOF'
import json, os, sys
table_dir, version, columns, data_file = sys.argv[1], int(sys.argv[2]), json.loads(sys.argv[3]), sys.argv[4]
fields = [{'name': name, 'type': type, 'nullable': True, 'metadata': {}} for name, type in columns]
actions = []
if version == 0:
    actions.append({'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}})
actions.append({'metaData': {'id': '8c1a1f5e-0000-4000-8000-000000000001', 'format': {'provider': 'parquet', 'options': {}},
                             'schemaString': json.dumps({'type': 'struct', 'fields': fields}),
                             'partitionColumns': [], 'configuration': {}, 'createdTime': 1700000000000}})
actions.append({'add': {'path': data_file, 'partitionValues': {}, 'size': os.path.getsize(os.path.join(table_dir, data_file)),
                        'modificationTime': 1700000000000 + version, 'dataChange': True}})
with open(os.path.join(table_dir, '_delta_log', '%020d.json' % version), 'w') as f:
    f.write('\n'.join(json.dumps(action) for action in actions) + '\n')
EOF
}

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${TABLE_DIR}/part-0.parquet', Parquet, 'a Nullable(Int64)') SELECT number + 1 FROM numbers(3);
    INSERT INTO FUNCTION file('${TABLE_DIR}/part-1.parquet', Parquet, 'a Nullable(Int64), b Nullable(String)') SELECT number + 10, concat('v', toString(number)) FROM numbers(2);
"
write_commit 0 '[["a", "long"]]' part-0.parquet

CATALOG="DataLakeCatalog('http://127.0.0.1:${PORT}/api/2.1/unity-catalog')"
SETTINGS="catalog_type = 'unity', warehouse = 'unity', vended_credentials = 0"
${CLICKHOUSE_CLIENT} --allow_database_unity_catalog 1 --query "
    CREATE DATABASE ${DB}_legacy ENGINE = ${CATALOG} SETTINGS ${SETTINGS};
    CREATE DATABASE ${DB}_legacy_compaction ENGINE = ${CATALOG} SETTINGS ${SETTINGS}, allow_experimental_iceberg_compaction = 1;
    CREATE DATABASE ${DB}_v2 ENGINE = ${CATALOG} SETTINGS ${SETTINGS}, use_unity_catalog_v2 = 1;
    CREATE DATABASE ${DB}_v2_compaction ENGINE = ${CATALOG} SETTINGS ${SETTINGS}, use_unity_catalog_v2 = 1, allow_experimental_iceberg_compaction = 1;
"

# A Delta table takes its columns from the catalog unless delta_lake_reload_schema_for_consistency is enabled.
# Parallel replicas would build the table anew for every query, which would hide a table kept from an earlier query.
read_all() {
    for name in ${DATABASES}; do
        echo "${name}: $(${CLICKHOUSE_CLIENT} --delta_lake_reload_schema_for_consistency 0 --parallel_replicas_for_cluster_engines 0 \
            --query "SELECT * FROM ${DB}_${name}.\`default.t\` ORDER BY a FORMAT CSVWithNames" | paste -sd' ' -)"
    done
}

echo "-- before the column is added"
read_all

write_commit 1 '[["a", "long"], ["b", "string"]]' part-1.parquet
echo '[["a", "long"], ["b", "string"]]' > "${STATE_FILE}"

echo "-- after the column is added"
read_all
