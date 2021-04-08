#!/usr/bin/env bash
# shellcheck disable=SC2016

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS template";
$CLICKHOUSE_CLIENT --query="CREATE TABLE template (s1 String, s2 String, \`s 3\` String, \"s 4\" String, n UInt64, d Date) ENGINE = Memory";
$CLICKHOUSE_CLIENT --query="INSERT INTO template VALUES
('qwe,rty', 'as\"df''gh', '', 'zx\ncv\tbn m', 123, '2016-01-01'),\
('as\"df''gh', '', 'zx\ncv\tbn m', 'qwe,rty', 456, '2016-01-02'),\
('', 'zx\ncv\tbn m', 'qwe,rty', 'as\"df''gh',  9876543210, '2016-01-03'),\
('zx\ncv\tbn m', 'qwe,rty', 'as\"df''gh', '', 789, '2016-01-04')";

echo -ne '{prefix} \n${data:None}\n------\n${totals:}\n------\n${min}\n------\n${max}\n${rows:Escaped} rows\nbefore limit ${rows_before_limit:XML}\nread ${rows_read:Escaped} $$ suffix $$' > "$CURDIR"/00937_template_output_format_resultset.tmp
echo -ne 'n:\t${n:JSON}, s1:\t${0:Escaped}, s2:\t${s2:Quoted}, s3:\t${`s 3`:JSON}, s4:\t${"s 4":CSV}, d:\t${d:Escaped}, n:\t${n:Raw}\t' > "$CURDIR"/00937_template_output_format_row.tmp

$CLICKHOUSE_CLIENT --query="SELECT * FROM template GROUP BY s1, s2, \`s 3\`, \"s 4\", n, d WITH TOTALS ORDER BY n LIMIT 4 FORMAT Template SETTINGS extremes = 1,\
format_template_resultset = '$CURDIR/00937_template_output_format_resultset.tmp', \
format_template_row = '$CURDIR/00937_template_output_format_row.tmp', \
format_template_rows_between_delimiter = ';\n'";

$CLICKHOUSE_CLIENT --query="DROP TABLE template";
rm "$CURDIR"/00937_template_output_format_resultset.tmp "$CURDIR"/00937_template_output_format_row.tmp
