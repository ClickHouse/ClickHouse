#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs IcebergLocal (USE_AVRO) and Iceberg writes.

# An Iceberg position delete file without the required `pos` or `file_path` column
# is rejected with ICEBERG_SPECIFICATION_VIOLATION naming the missing column.
# https://github.com/ClickHouse/ClickHouse/issues/123510

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
trap 'rm -rf "${TABLE_PATH}"' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE id < 3"

find "${TABLE_PATH}data" -name '*-deletes.parquet' | wc -l
DELETE_FILE=$(find "${TABLE_PATH}data" -name '*-deletes.parquet')

# The delete file is applied before it is corrupted.
for roaring in 0 1; do
    ${CLICKHOUSE_CLIENT} --use_roaring_bitmap_iceberg_positional_deletes=${roaring} --query "SELECT count(), sum(id) FROM ${TABLE}"
done

# Renames a column in the Parquet footer schema of the delete file, keeping the file size.
rename_column()
{
    python3 - "${DELETE_FILE}" "$1" "$2" <<'EOF'
import struct, sys
path, old, new = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
data = bytearray(open(path, 'rb').read())
footer = len(data) - 8 - struct.unpack('<I', data[-8:-4])[0]
i = data.find(bytes([len(old)]) + old, footer)
if len(old) != len(new) or i < 0:
    sys.exit(f'cannot rename {old} to {new} in the footer of {path}')
data[i + 1:i + 1 + len(old)] = new
open(path, 'wb').write(data)
EOF
}

select_error()
{
    ${CLICKHOUSE_CLIENT} --use_roaring_bitmap_iceberg_positional_deletes="$1" --query "SELECT count(), sum(id) FROM ${TABLE}" 2>&1 \
        | grep -m1 -o -e "has no column '[a-z_]*'" -e "(ICEBERG_SPECIFICATION_VIOLATION)" | paste -sd ' ' -
}

rename_column pos pox
for roaring in 0 1; do
    echo "pos roaring=${roaring}:"
    select_error ${roaring}
done

rename_column pox pos
rename_column file_path file_patx
echo "file_path:"
select_error 0

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
