#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS template1";
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS template2";
$CLICKHOUSE_CLIENT --query="CREATE TABLE template1 (s1 String, s2 String, s3 String, s4 String, n UInt64, d Date) ENGINE = Memory";
$CLICKHOUSE_CLIENT --query="CREATE TABLE template2 (s1 String, s2 String, s3 String, s4 String, n UInt64, d Date) ENGINE = Memory";

echo "==== check escaping ===="

echo "{prefix} 
n:	123, s1:	qwe,rty	, s2:	'as\"df\\'gh', s3:	\"\", s4:	\"zx
cv	bn m\", d:	2016-01-01	;
n:	456, s1:	as\"df\\'gh	, s2:	'', s3:	\"zx\\ncv\\tbn m\", s4:	\"qwe,rty\", d:	2016-01-02	;
n:	9876543210, s1:		, s2:	'zx\\ncv\\tbn m', s3:	\"qwe,rty\", s4:	\"as\"\"df'gh\", d:	2016-01-03	;
n:	789, s1:	zx\\ncv\\tbn m	, s2:	'qwe,rty', s3:	\"as\\\"df'gh\", s4:	\"\", d:	2016-01-04	
 $ suffix $" | $CLICKHOUSE_CLIENT --query="INSERT INTO template1 FORMAT Template SETTINGS \
format_schema = '{prefix} \n\${data}\n \$\$ suffix \$\$\n', \
format_schema_rows = 'n:\t\${n:Escaped}, s1:\t\${s1:Escaped}\t, s2:\t\${s2:Quoted}, s3:\t\${s3:JSON}, s4:\t\${s4:CSV}, d:\t\${d:Escaped}\t', \
format_schema_rows_between_delimiter = ';\n'";

$CLICKHOUSE_CLIENT --query="SELECT * FROM template1 ORDER BY n FORMAT CSV";

echo "==== parse json (sophisticated template) ===="

$CLICKHOUSE_CLIENT --query="SELECT * FROM template1 ORDER BY n FORMAT JSON" | $CLICKHOUSE_CLIENT --query="INSERT INTO template2 FORMAT TemplateIgnoreSpaces SETTINGS \
format_schema = '{\${:}\"meta\"\${:}:\${:}[\${:}{\${:}\"name\"\${:}:\${:}\"s1\"\${:},\${:}\"type\"\${:}:\${:}\"String\"\${:}}\${:},\${:}{\${:}\"name\"\${:}:\${:}\"s2\"\${:},\${:}\"type\"\${:}:\${:}\"String\"\${:}}\${:},\${:}{\${:}\"name\"\${:}:\${:}\"s3\"\${:},\${:}\"type\"\${:}:\${:}\"String\"\${:}}\${:},\${:}{\${:}\"name\"\${:}:\${:}\"s4\"\${:},\${:}\"type\"\${:}:\${:}\"String\"\${:}}\${:},\${:}{\${:}\"name\"\${:}:\${:}\"n\"\${:},\${:}\"type\"\${:}:\${:}\"UInt64\"\${:}}\${:},\${:}{\${:}\"name\"\${:}:\${:}\"d\"\${:},\${:}\"type\"\${:}:\${:}\"Date\"\${:}}\${:}]\${:},\${:}\"data\"\${:}:\${:}[\${data}]\${:},\${:}\"rows\"\${:}:\${:}\${:CSV}\${:},\${:}\"statistics\"\${:}:\${:}{\${:}\"elapsed\"\${:}:\${:}\${:CSV}\${:},\${:}\"rows_read\"\${:}:\${:}\${:CSV}\${:},\${:}\"bytes_read\"\${:}:\${:}\${:CSV}\${:}}\${:}}', \
format_schema_rows = '{\${:}\"s1\"\${:}:\${:}\${s1:JSON}\${:},\${:}\"s2\"\${:}:\${:}\${s2:JSON}\${:},\${:}\"s3\"\${:}:\${:}\${s3:JSON}\${:},\${:}\"s4\"\${:}:\${:}\${s4:JSON}\${:},\${:}\"n\"\${:}:\${:}\${n:JSON}\${:},\${:}\"d\"\${:}:\${:}\${d:JSON}\${:}\${:}}', \
format_schema_rows_between_delimiter = ','";

$CLICKHOUSE_CLIENT --query="SELECT * FROM template2 ORDER BY n FORMAT CSV";
$CLICKHOUSE_CLIENT --query="TRUNCATE TABLE template2";

echo "==== parse json ===="

$CLICKHOUSE_CLIENT --query="SELECT * FROM template1 ORDER BY n FORMAT JSON" | $CLICKHOUSE_CLIENT --query="INSERT INTO template2 FORMAT TemplateIgnoreSpaces SETTINGS \
format_schema = '{\${:}\"meta\"\${:}:\${:JSON},\${:}\"data\"\${:}:\${:}[\${data}]\${:},\${:}\"rows\"\${:}:\${:JSON},\${:}\"statistics\"\${:}:\${:JSON}\${:}}', \
format_schema_rows = '{\${:}\"s1\"\${:}:\${:}\${s3:JSON}\${:},\${:}\"s2\"\${:}:\${:}\${:JSON}\${:},\${:}\"s3\"\${:}:\${:}\${s1:JSON}\${:},\${:}\"s4\"\${:}:\${:}\${:JSON}\${:},\${:}\"n\"\${:}:\${:}\${n:JSON}\${:},\${:}\"d\"\${:}:\${:}\${d:JSON}\${:}\${:}}', \
format_schema_rows_between_delimiter = ','";

$CLICKHOUSE_CLIENT --query="SELECT * FROM template2 ORDER BY n FORMAT CSV";

$CLICKHOUSE_CLIENT --query="DROP TABLE template1";
$CLICKHOUSE_CLIENT --query="DROP TABLE template2";
