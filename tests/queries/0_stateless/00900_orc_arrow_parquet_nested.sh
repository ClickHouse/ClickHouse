#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS nested_table"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS nested_nested_table"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE nested_table (table Nested(elem1 Int32, elem2 String, elem3 Float32)) engine=Memory"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE nested_nested_table (table Nested(elem1 Int32, elem2 String, elem3 Float32, nested Nested(elem1 Int32, elem2 String, elem3 Float32))) engine=Memory"


formats=('Arrow' 'Parquet' 'ORC')
format_files=('arrow' 'parquet' 'orc')

for ((i = 0; i < 3; i++)) do
    echo ${formats[i]}

    ${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE nested_table"
    cat $CUR_DIR/data_orc_arrow_parquet_nested/nested_table.${format_files[i]} | ${CLICKHOUSE_CLIENT} -q "INSERT INTO nested_table SETTINGS input_format_${format_files[i]}_import_nested = 1 FORMAT ${formats[i]}"

    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM nested_table"


    ${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE nested_nested_table"
    cat $CUR_DIR/data_orc_arrow_parquet_nested/nested_nested_table.${format_files[i]} | ${CLICKHOUSE_CLIENT} -q "INSERT INTO nested_nested_table SETTINGS input_format_${format_files[i]}_import_nested = 1 FORMAT ${formats[i]}"

    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM nested_nested_table"
done

${CLICKHOUSE_CLIENT} --query="DROP TABLE nested_table"
${CLICKHOUSE_CLIENT} --query="DROP TABLE nested_nested_table"
