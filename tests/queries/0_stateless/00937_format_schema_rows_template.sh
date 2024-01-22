#!/usr/bin/env bash
# shellcheck disable=SC2016

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test format_schema_rows_template setting 

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS template";
$CLICKHOUSE_CLIENT --query="CREATE TABLE template (question String, answer String, likes UInt64, date Date) ENGINE = Memory";
$CLICKHOUSE_CLIENT --query="INSERT INTO template VALUES
('How awesome is clickhouse?', 'unbelievably awesome!', 456, '2016-01-02'),\
('How fast is clickhouse?', 'Lightning fast!', 9876543210, '2016-01-03'),\
('Is it opensource', 'of course it is!', 789, '2016-01-04')";

$CLICKHOUSE_CLIENT --query="SELECT * FROM template GROUP BY question, answer, likes, date WITH TOTALS ORDER BY date LIMIT 3 FORMAT Template SETTINGS \
format_schema_rows_template = 'Question: \${question:Quoted}, Answer: \${answer:Quoted}, Number of Likes: \${likes:Raw}, Date: \${date:Raw}', \
format_template_rows_between_delimiter = ';\n'";

echo -e "\n"

# Test that if both format_schema_rows_template setting and format_template_row are provided, error is thrown 

echo -ne 'Question: ${question:Quoted}, Answer: ${answer:Quoted}, Number of Likes: ${likes:Raw}, Date: ${date:Raw}' > "$CURDIR"/00937_template_output_format_row.tmp
$CLICKHOUSE_CLIENT --query="SELECT * FROM template GROUP BY question, answer, likes, date WITH TOTALS ORDER BY date LIMIT 3 FORMAT Template SETTINGS \
format_template_row = '$CURDIR/00937_template_output_format_row.tmp', \
format_schema_rows_template = 'Question: \${question:Quoted}, Answer: \${answer:Quoted}, Number of Likes: \${likes:Raw}, Date: \${date:Raw}', \
format_template_rows_between_delimiter = ';\n'"; -- { serverError 474 }

$CLICKHOUSE_CLIENT --query="DROP TABLE template";
rm "$CURDIR"/00937_template_output_format_row.tmp