#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_bool_fixed"

rm -rf "${TABLE_PATH}"
mkdir -p "${TABLE_PATH}/metadata" "${TABLE_PATH}/data"

# ClickHouse cannot declare `boolean` or `fixed[N]` in CREATE, so the empty table is stated the way
# another engine writes it and only the INSERT below is ClickHouse's.
cat > "${TABLE_PATH}/metadata/v1.metadata.json" <<EOF
{
  "format-version": 2,
  "table-uuid": "6b2c1e1c-6d16-4f5e-9b2a-3f5b9c7a1d20",
  "location": "${TABLE_PATH}",
  "last-updated-ms": 1700000000000,
  "last-column-id": 3,
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "i", "required": false, "type": "int"},
        {"id": 2, "name": "b", "required": false, "type": "boolean"},
        {"id": 3, "name": "f", "required": false, "type": "fixed[4]"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "last-partition-id": 999,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {},
  "snapshots": [],
  "snapshot-log": [],
  "metadata-log": [],
  "refs": {},
  "last-sequence-number": 0
}
EOF

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    SET max_insert_threads = 1;
    CREATE TABLE bool_fixed ENGINE = IcebergLocal('${TABLE_PATH}');
    INSERT INTO bool_fixed VALUES (1, true, 'abcd'), (2, false, 'zzzz');
"

echo '--- the column types come from the Iceberg schema ---'
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE bool_fixed FORMAT TSV;" | cut -f1,2

MANIFEST=$(find "${TABLE_PATH}/metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f | sort | head -1)

echo '--- a boolean bound is one byte and a fixed bound is the raw value ---'
${CLICKHOUSE_CLIENT} --query "
    WITH
        tupleElement(data_file, 'lower_bounds') AS lower,
        tupleElement(data_file, 'upper_bounds') AS upper
    SELECT
        min(hex(arrayFirst(x -> x.1 = 2, lower).2))    AS bool_lower,
        max(hex(arrayFirst(x -> x.1 = 2, upper).2))    AS bool_upper,
        min(arrayFirst(x -> x.1 = 3, lower).2)         AS fixed_lower,
        max(arrayFirst(x -> x.1 = 3, upper).2)         AS fixed_upper,
        max(length(arrayFirst(x -> x.1 = 2, lower).2)) AS bool_bound_bytes,
        max(length(arrayFirst(x -> x.1 = 3, lower).2)) AS fixed_bound_bytes,
        max(length(arrayFirst(x -> x.1 = 1, lower).2)) AS int_bound_bytes
    FROM file('${MANIFEST}', Avro)
    FORMAT Vertical;
"

echo '--- the data reads back unchanged ---'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM bool_fixed ORDER BY i FORMAT TSV;"

rm -rf "${TABLE_PATH}"
