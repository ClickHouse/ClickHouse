#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Generate a deeply nested Avro file that would cause stack overflow
# without proper recursion depth checks.
AVRO_FILE="${CLICKHOUSE_TMP}/deeply_nested_${CLICKHOUSE_DATABASE}.avro"

python3 -c "
import sys, avro.schema, avro.datafile, avro.io
sys.setrecursionlimit(20000)

depth = 2000

# Build the JSON schema string iteratively to avoid json.dumps recursion.
schema_str = '{\"type\": \"int\"}'
for i in range(depth):
    schema_str = '{\"type\": \"array\", \"items\": ' + schema_str + '}'
schema_str = '{\"type\": \"record\", \"name\": \"test\", \"fields\": [{\"name\": \"nested\", \"type\": ' + schema_str + '}]}'
schema = avro.schema.parse(schema_str)

data = 42
for i in range(depth):
    data = [data]

with open('${AVRO_FILE}', 'wb') as f:
    writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)
    writer.append({'nested': data})
    writer.close()
"

# Should not crash (SIGSEGV). May succeed or fail with TOO_DEEP_RECURSION depending on build type.
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${AVRO_FILE}', 'Avro') FORMAT Null" > /dev/null 2>&1
[ $? -lt 128 ] || echo "CRASHED"

${CLICKHOUSE_LOCAL} -q "DESC file('${AVRO_FILE}', 'Avro')" > /dev/null 2>&1
[ $? -lt 128 ] || echo "CRASHED"

rm -f "${AVRO_FILE}"
