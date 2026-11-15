#!/usr/bin/env bash
# shellcheck disable=SC2016

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test format_template_row_format setting 

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS template";
$CLICKHOUSE_CLIENT --query="CREATE TABLE template (question String, answer String, likes UInt64, date Date) ENGINE = Memory";
$CLICKHOUSE_CLIENT --query="INSERT INTO template VALUES
('How awesome is clickhouse?', 'unbelievably awesome!', 456, '2016-01-02'),\
('How fast is clickhouse?', 'Lightning fast!', 9876543210, '2016-01-03'),\
('Is it opensource?', 'of course it is!', 789, '2016-01-04')";

$CLICKHOUSE_CLIENT --query="SELECT * FROM template GROUP BY question, answer, likes, date WITH TOTALS ORDER BY date LIMIT 3 FORMAT Template SETTINGS \
format_template_row_format = 'Question: \${question:Quoted}, Answer: \${answer:Quoted}, Number of Likes: \${likes:Raw}, Date: \${date:Raw}', \
format_template_rows_between_delimiter = ';\n'";

echo -e "\n"

# Test that if both format_template_row_format setting and format_template_row are provided, error is thrown
row_format_file="$CURDIR"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"_template_output_format_row.tmp
echo -ne 'Question: ${question:Quoted}, Answer: ${answer:Quoted}, Number of Likes: ${likes:Raw}, Date: ${date:Raw}' > $row_format_file
$CLICKHOUSE_CLIENT --multiline --query "SELECT * FROM template GROUP BY question, answer, likes, date WITH TOTALS ORDER BY date LIMIT 3 FORMAT Template SETTINGS \
format_template_row = '$row_format_file', \
format_template_row_format = 'Question: \${question:Quoted}, Answer: \${answer:Quoted}, Number of Likes: \${likes:Raw}, Date: \${date:Raw}', \
format_template_rows_between_delimiter = ';\n'; --{clientError 474}"

# Test format_template_resultset_format setting 

$CLICKHOUSE_CLIENT --query="SELECT * FROM template GROUP BY question, answer, likes, date WITH TOTALS ORDER BY date LIMIT 3 FORMAT Template SETTINGS \
format_template_row_format = 'Question: \${question:Quoted}, Answer: \${answer:Quoted}, Number of Likes: \${likes:Raw}, Date: \${date:Raw}', \
format_template_resultset_format = '===== Results ===== \n\${data}\n===================\n', \
format_template_rows_between_delimiter = ';\n'";

# Test that if both format_template_result_format setting and format_template_resultset are provided, error is thrown
resultset_output_file="$CURDIR"/"$CLICKHOUSE_TEST_UNIQUE_NAME"_template_output_format_resultset.tmp
echo -ne '===== Resultset ===== \n \${data} \n ===============' > $resultset_output_file
$CLICKHOUSE_CLIENT --multiline --query "SELECT * FROM template GROUP BY question, answer, likes, date WITH TOTALS ORDER BY date LIMIT 3 FORMAT Template SETTINGS \
format_template_resultset = '$resultset_output_file', \
format_template_resultset_format = '===== Resultset ===== \n \${data} \n ===============', \
format_template_row_format = 'Question: \${question:Quoted}, Answer: \${answer:Quoted}, Number of Likes: \${likes:Raw}, Date: \${date:Raw}', \
format_template_rows_between_delimiter = ';\n'; --{clientError 474}"

$CLICKHOUSE_CLIENT --query="DROP TABLE template";
rm $row_format_file
rm $resultset_output_file
