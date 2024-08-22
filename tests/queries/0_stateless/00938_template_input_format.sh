#!/usr/bin/env bash
# shellcheck disable=SC2016,SC2028

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CURDIR=$CURDIR/${CLICKHOUSE_DATABASE}
mkdir -p $CURDIR

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS template1";
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS template2";
$CLICKHOUSE_CLIENT --query="CREATE TABLE template1 (s1 String, s2 String, s3 String, s4 String, n UInt64, d Date) ENGINE = Memory";
$CLICKHOUSE_CLIENT --query="CREATE TABLE template2 (s1 String, s2 String, s3 String, s4 String, n UInt64, d Date) ENGINE = Memory";

echo "==== check escaping ===="
echo -ne '{prefix} \n${data}\n $$ suffix $$\n' > "$CURDIR"/00938_template_input_format_resultset.tmp
echo -ne 'n:\t${n:Escaped}, s1:\t${0:Escaped}\t, s2:\t${1:Quoted}, s3:\t${s3:JSON}, s4:\t${3:CSV}, d:\t${d:Escaped}\t' > "$CURDIR"/00938_template_input_format_row.tmp

echo "{prefix}"' '"
n:	123, s1:	qwe,rty	, s2:	'as\"df\\'gh', s3:	\"\", s4:	\"zx
cv	bn m\", d:	2016-01-01	;
n:	456, s1:	as\"df\\'gh	, s2:	'', s3:	\"zx\\ncv\\tbn m\", s4:	\"qwe,rty\", d:	2016-01-02	;
n:	9876543210, s1:		, s2:	'zx\\ncv\\tbn m', s3:	\"qwe,rty\", s4:	\"as\"\"df'gh\", d:	2016-01-03	;
n:	789, s1:	zx\\ncv\\tbn m	, s2:	'qwe,rty', s3:	\"as\\\"df'gh\", s4:	\"\", d:	2016-01-04"$'\t'"
 $ suffix $" | $CLICKHOUSE_CLIENT --query="INSERT INTO template1 SETTINGS \
format_template_resultset = '$CURDIR/00938_template_input_format_resultset.tmp', \
format_template_row = '$CURDIR/00938_template_input_format_row.tmp', \
format_template_rows_between_delimiter = ';\n' \
FORMAT Template";

$CLICKHOUSE_CLIENT --query="SELECT * FROM template1 ORDER BY n FORMAT CSV";

echo "==== parse json (sophisticated template) ===="
echo -ne '{${:}"meta"${:}:${:}[${:}{${:}"name"${:}:${:}"s1"${:},${:}"type"${:}:${:}"String"${:}}${:},${:}{${:}"name"${:}:${:}"s2"${:},${:}"type"${:}:${:}"String"${:}}${:},${:}{${:}"name"${:}:${:}"s3"${:},${:}"type"${:}:${:}"String"${:}}${:},${:}{${:}"name"${:}:${:}"s4"${:},${:}"type"${:}:${:}"String"${:}}${:},${:}{${:}"name"${:}:${:}"n"${:},${:}"type"${:}:${:}"UInt64"${:}}${:},${:}{${:}"name"${:}:${:}"d"${:},${:}"type"${:}:${:}"Date"${:}}${:}]${:},${:}"data"${:}:${:}[${data}]${:},${:}"rows"${:}:${:}${:CSV}${:},${:}"statistics"${:}:${:}{${:}"elapsed"${:}:${:}${:CSV}${:},${:}"rows_read"${:}:${:}${:CSV}${:},${:}"bytes_read"${:}:${:}${:CSV}${:}}${:}}' > "$CURDIR"/00938_template_input_format_resultset.tmp
echo -ne '{${:}"s1"${:}:${:}${s1:JSON}${:},${:}"s2"${:}:${:}${s2:JSON}${:},${:}"s3"${:}:${:}${s3:JSON}${:},${:}"s4"${:}:${:}${s4:JSON}${:},${:}"n"${:}:${:}${n:JSON}${:},${:}"d"${:}:${:}${d:JSON}${:}${:}}' > "$CURDIR"/00938_template_input_format_row.tmp

$CLICKHOUSE_CLIENT --query="SELECT * FROM template1 ORDER BY n FORMAT JSON" | $CLICKHOUSE_CLIENT --query="INSERT INTO template2 SETTINGS \
format_template_resultset = '$CURDIR/00938_template_input_format_resultset.tmp', \
format_template_row = '$CURDIR/00938_template_input_format_row.tmp', \
format_template_rows_between_delimiter = ',' \
FORMAT TemplateIgnoreSpaces";

$CLICKHOUSE_CLIENT --query="SELECT * FROM template2 ORDER BY n FORMAT CSV";
$CLICKHOUSE_CLIENT --query="TRUNCATE TABLE template2";

echo "==== parse json ===="

echo -ne '{${:}"meta"${:}:${:JSON},${:}"data"${:}:${:}[${data}]${:},${:}"rows"${:}:${:JSON},${:}"statistics"${:}:${:JSON}${:}}' > "$CURDIR"/00938_template_input_format_resultset.tmp
echo -ne '{${:}"s1"${:}:${:}${s3:JSON}${:},${:}"s2"${:}:${:}${:JSON}${:},${:}"s3"${:}:${:}${s1:JSON}${:},${:}"s4"${:}:${:}${:JSON}${:},${:}"n"${:}:${:}${n:JSON}${:},${:}"d"${:}:${:}${d:JSON}${:}${:}}' > "$CURDIR"/00938_template_input_format_row.tmp
$CLICKHOUSE_CLIENT --query="SELECT * FROM template1 ORDER BY n FORMAT JSON" | $CLICKHOUSE_CLIENT --query="INSERT INTO template2 SETTINGS \
format_template_resultset = '$CURDIR/00938_template_input_format_resultset.tmp', \
format_template_row = '$CURDIR/00938_template_input_format_row.tmp', \
format_template_rows_between_delimiter = ',' \
FORMAT TemplateIgnoreSpaces";

$CLICKHOUSE_CLIENT --query="SELECT * FROM template2 ORDER BY n FORMAT CSV";

echo "==== check raw ===="

echo -ne '{prefix} \n${data}\n $$ suffix $$\n' > "$CURDIR"/00938_template_input_format_resultset.tmp
echo -ne 'n:\t${n:Escaped}, s1:\t${0:Raw}\t, s2:\t${1:Quoted}, s3:\t${s3:JSON}, s4:\t${3:CSV}, d:\t${d:Escaped}\t' > "$CURDIR"/00938_template_input_format_row.tmp


$CLICKHOUSE_CLIENT --query="TRUNCATE TABLE template1";

echo "{prefix}"' '"
n:	123, s1:	qwe,rty	, s2:	'as\"df\\'gh', s3:	\"\", s4:	\"zx
cv	bn m\", d:	2016-01-01	;
n:	456, s1:	as\"df\\'gh	, s2:	'', s3:	\"zx\\ncv\\tbn m\", s4:	\"qwe,rty\", d:	2016-01-02	;
n:	9876543210, s1:		, s2:	'zx\\ncv\\tbn m', s3:	\"qwe,rty\", s4:	\"as\"\"df'gh\", d:	2016-01-03	;
n:	789, s1:	zx\cv\bn m	, s2:	'qwe,rty', s3:	\"as\\\"df'gh\", s4:	\"\", d:	2016-01-04"$'\t'"
 $ suffix $" | $CLICKHOUSE_CLIENT --query="INSERT INTO template1 SETTINGS \
format_template_resultset = '$CURDIR/00938_template_input_format_resultset.tmp', \
format_template_row = '$CURDIR/00938_template_input_format_row.tmp', \
format_template_rows_between_delimiter = ';\n' \
FORMAT Template";

$CLICKHOUSE_CLIENT --query="SELECT * FROM template1 ORDER BY n FORMAT CSV";



$CLICKHOUSE_CLIENT --query="DROP TABLE template1";
$CLICKHOUSE_CLIENT --query="DROP TABLE template2";
rm "$CURDIR"/00938_template_input_format_resultset.tmp "$CURDIR"/00938_template_input_format_row.tmp

echo -ne '\${a:Escaped},\${b:Escaped}\n' > "$CURDIR"/00938_template_input_format_row.tmp
echo -ne "a,b\nc,d\n" | $CLICKHOUSE_LOCAL --structure "a String, b String"  --input-format Template \
    --format_template_row "$CURDIR"/00938_template_input_format_row.tmp --format_template_rows_between_delimiter '' \
    -q 'select * from table' 2>&1| grep -Fac "'Escaped' serialization requires delimiter"
echo -ne '\${a:Escaped},\${:Escaped}\n' > "$CURDIR"/00938_template_input_format_row.tmp
echo -ne "a,b\nc,d\n" | $CLICKHOUSE_LOCAL --structure "a String"  --input-format Template \
    --format_template_row "$CURDIR"/00938_template_input_format_row.tmp --format_template_rows_between_delimiter '' \
    -q 'select * from table' 2>&1| grep -Fac "'Escaped' serialization requires delimiter"
rm "$CURDIR"/00938_template_input_format_row.tmp

