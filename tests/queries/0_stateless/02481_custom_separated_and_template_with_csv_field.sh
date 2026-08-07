#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "1||2&&3||4&&" | $CLICKHOUSE_LOCAL --input-format=CustomSeparated --format_custom_field_delimiter='||' --format_custom_row_after_delimiter='&&' --format_custom_escaping_rule='CSV' -q "select * from table"

echo "1||2|||3||4|||" | $CLICKHOUSE_LOCAL --input-format=CustomSeparated --format_custom_field_delimiter='||' --format_custom_row_after_delimiter='|||' --format_custom_escaping_rule='CSV' -q "select * from table"

echo "ab|c||de&f&&" | $CLICKHOUSE_LOCAL --input-format=CustomSeparated --format_custom_field_delimiter='||' --format_custom_row_after_delimiter='&&' --format_custom_escaping_rule='CSV' -q "select * from table"

echo -e "\${column_1:CSV}||\${column_2:CSV}**\${column_3:CSV}&&" > row_format_02481

echo -e "ab|c||de*f**gh&k&&\n|av||*ad**&ad&&" | $CLICKHOUSE_LOCAL -q "select * from table" --input-format=Template --format_template_row='row_format_02481' --format_template_rows_between_delimiter ""

rm row_format_02481

