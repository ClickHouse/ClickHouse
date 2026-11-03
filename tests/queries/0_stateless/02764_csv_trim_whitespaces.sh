#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

BOOLS=('false' 'true')

WHITESPACES=(      '\t'  ' '      '\t ')
WHITESPACES_NAMES=('tab' 'space'  'tab_space')

DATA_TYPES=(  'Int8'  'Int16' 'Float32' 'Float64'  'Date'        'Date32'        'DateTime'              'DateTime64')
DATA_VALUES=( '8'     '16'    '32.32'   '64.64'    '2023-05-22'  '2023-05-22'    '2023-05-22 00:00:00'   '2023-05-22 00:00:00.000')

for trim in "${BOOLS[@]}"
do
    for wsIndex in "${!WHITESPACES[@]}";
    do
        whitespace=${WHITESPACES[$wsIndex]}
        whitespace_name=${WHITESPACES_NAMES[$wsIndex]}
        echo -e "${whitespace}trim_${trim}_${whitespace_name}_left"                                              | $CLICKHOUSE_LOCAL --input_format_csv_trim_whitespaces=${trim} --input-format="CSV" -q "select * from table FORMAT CSV"
        echo -e "trim_${trim}_${whitespace_name}_right${whitespace}"                                             | $CLICKHOUSE_LOCAL --input_format_csv_trim_whitespaces=${trim} --input-format="CSV" -q "select * from table FORMAT CSV"
        echo -e "trim_${trim}_${whitespace}${whitespace_name}_middle"                                            | $CLICKHOUSE_LOCAL --input_format_csv_trim_whitespaces=${trim} --input-format="CSV" -q "select * from table FORMAT CSV"
        echo -e "${whitespace}trim_${trim}_${whitespace}${whitespace_name}_everywhere${whitespace}"              | $CLICKHOUSE_LOCAL --input_format_csv_trim_whitespaces=${trim} --input-format="CSV" -q "select * from table FORMAT CSV"
        echo -e "${whitespace}trim_${trim}_fixed_string_${whitespace}${whitespace_name}_everywhere${whitespace}" | $CLICKHOUSE_LOCAL -S "c1 FixedString(64)" --input_format_csv_trim_whitespaces=${trim} --input-format="CSV" -q "select toString(c1) from table FORMAT CSV"
        echo -e "\"${whitespace}quoted_trim_${trim}_${whitespace}${whitespace_name}_everywhere${whitespace}\""   | $CLICKHOUSE_LOCAL --input_format_csv_trim_whitespaces=${trim} --input-format="CSV" -q "select * from table FORMAT CSV"
        echo -e "${whitespace}trim_${trim}_csv_field1${whitespace},123${whitespace},5.0${whitespace},${whitespace}12.0123,\"${whitespace}quoted_string1\"\n${whitespace}trim_${trim}_csv_field2${whitespace},${whitespace}321${whitespace},${whitespace}0.5,21.321${whitespace},\"${whitespace}quoted_${whitespace}string2${whitespace}\"${whitespace}" | $CLICKHOUSE_LOCAL --input_format_csv_trim_whitespaces=${trim} --input-format="CSV" -q "select * from table FORMAT CSV"
        echo -e "${whitespace}trim_${trim}_csv_field1_with_${whitespace}structure${whitespace},${whitespace}123,${whitespace}5.0${whitespace},12.0123${whitespace},\"${whitespace}quoted_string${whitespace}\"\n${whitespace}trim_${trim}_csv_field2_with_structure${whitespace},${whitespace}321${whitespace},0.5,21.321,\"${whitespace}quoted_${whitespace}_string2${whitespace}\"${whitespace}" | $CLICKHOUSE_LOCAL -S "c1 String, c2 Int32, c3 Float, c4 Double, c5 String" --input_format_csv_trim_whitespaces=${trim} --input-format="CSV" -q "select * from table FORMAT CSV"
    done

    for type_index in "${!DATA_TYPES[@]}";
    do
        type=${DATA_TYPES[$type_index]}
        value=${DATA_VALUES[$type_index]}
        echo -e "\t ${value} \t" | $CLICKHOUSE_LOCAL -S "c1 ${type}" --input-format="CSV" --input_format_csv_trim_whitespaces=${trim} -q "select * from table FORMAT CSV"
        echo -e "\t ${value} \t" | $CLICKHOUSE_LOCAL -S "c1 Nullable(${type})" --input-format="CSV" --input_format_csv_trim_whitespaces=${trim} -q "select * from table FORMAT CSV"
    done
done

## Custom CSV tested with input_format_csv_trim_whitespaces = false.
## Custom CSV with input_format_csv_trim_whitespaces=true doesn't trim whitespaces from the left side at the moment
for wsIndex in "${!WHITESPACES[@]}";
do
    whitespace=${WHITESPACES[$wsIndex]}
    whitespace_name=${WHITESPACES_NAMES[$wsIndex]}
    echo -e "${whitespace}custom_csv_${whitespace_name}_left"                                               | $CLICKHOUSE_LOCAL --input-format="CustomSeparated" --input_format_csv_trim_whitespaces=false --format_custom_escaping_rule=CSV --format_custom_field_delimiter=',' --format_csv_delimiter=',' -q "select * from table FORMAT CSV"
    echo -e "custom_csv_${whitespace_name}_right${whitespace}"                                              | $CLICKHOUSE_LOCAL --input-format="CustomSeparated" --input_format_csv_trim_whitespaces=false --format_custom_escaping_rule=CSV --format_custom_field_delimiter=',' --format_csv_delimiter=',' -q "select * from table FORMAT CSV"
    echo -e "custom_csv_${whitespace}${whitespace_name}_middle"                                             | $CLICKHOUSE_LOCAL --input-format="CustomSeparated" --input_format_csv_trim_whitespaces=false --format_custom_escaping_rule=CSV --format_custom_field_delimiter=',' --format_csv_delimiter=',' -q "select * from table FORMAT CSV"
    echo -e "${whitespace}custom_csv_${whitespace}${whitespace_name}_everywhere${whitespace}"               | $CLICKHOUSE_LOCAL --input-format="CustomSeparated" --input_format_csv_trim_whitespaces=false --format_custom_escaping_rule=CSV --format_custom_field_delimiter=',' --format_csv_delimiter=',' -q "select * from table FORMAT CSV"
    echo -e "${whitespace}custom_csv_fixed_string_${whitespace}${whitespace_name}_everywhere${whitespace}"  | $CLICKHOUSE_LOCAL -S "c1 FixedString(64)" --input-format="CustomSeparated"  --input_format_csv_trim_whitespaces=false --format_custom_escaping_rule=CSV --format_custom_field_delimiter=',' --format_csv_delimiter=',' -q "select toString(c1) from table FORMAT CSV"
    echo -e "\"${whitespace}quoted_custom_csv_${whitespace}${whitespace_name}_everywhere${whitespace}\""    | $CLICKHOUSE_LOCAL --input-format="CustomSeparated" --input_format_csv_trim_whitespaces=false --format_custom_escaping_rule=CSV --format_custom_field_delimiter=',' --format_csv_delimiter=',' -q "select * from table FORMAT CSV"

    echo -e "${whitespace}custom_csv_field_with_${whitespace}structure${whitespace},123,5.0,12.0123,\"${whitespace}custom_csv_quoted_string${whitespace}\"\n${whitespace}custom_csv_field2_with_structure${whitespace},321,0.5,21.321,\"${whitespace}custom_csv_quoted_${whitespace}_string2${whitespace}\"" | $CLICKHOUSE_LOCAL --input_format_csv_trim_whitespaces=false --input-format="CustomSeparated" --format_custom_escaping_rule=CSV --format_custom_field_delimiter=',' --format_csv_delimiter=',' -q "select * from table FORMAT CSV"
    echo -e "${whitespace}custom_csv_field_with_${whitespace}structure${whitespace},123,5.0,12.0123,\"${whitespace}custom_csv_quoted_string${whitespace}\"\n${whitespace}custom_csv_field2_with_structure${whitespace},321,0.5,21.321,\"${whitespace}custom_csv_quoted_${whitespace}_string2${whitespace}\"" | $CLICKHOUSE_LOCAL -S "c1 String, c2 Int32, c3 Float, c4 Double, c5 String" --input_format_csv_trim_whitespaces=false --input-format="CustomSeparated" --format_custom_escaping_rule=CSV --format_custom_field_delimiter=',' --format_csv_delimiter=',' -q "select * from table FORMAT CSV"
done
