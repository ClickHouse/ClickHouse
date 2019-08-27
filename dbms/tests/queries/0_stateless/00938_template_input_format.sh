#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS template";
$CLICKHOUSE_CLIENT --query="CREATE TABLE template (s1 String, s2 String, s3 String, s4 String, n UInt64, d Date) ENGINE = Memory";

echo "{prefix} 
n:	123, s1:	qwe,rty	, s2:	'as\"df\\'gh', s3:	\"\", s4:	\"zx
cv	bn m\", d:	2016-01-01	;
n:	456, s1:	as\"df\\'gh	, s2:	'', s3:	\"zx\\ncv\\tbn m\", s4:	\"qwe,rty\", d:	2016-01-02	;
n:	9876543210, s1:		, s2:	'zx\\ncv\\tbn m', s3:	\"qwe,rty\", s4:	\"as\"\"df'gh\", d:	2016-01-03	;
n:	789, s1:	zx\\ncv\\tbn m	, s2:	'qwe,rty', s3:	\"as\\\"df'gh\", s4:	\"\", d:	2016-01-04	
 $ suffix $" | $CLICKHOUSE_CLIENT --query="INSERT INTO template FORMAT Template SETTINGS format_schema = '{prefix} \n\${data}\n \$\$ suffix \$\$\n', format_schema_rows = 'n:\t\${n:Escaped}, s1:\t\${s1:Escaped}\t, s2:\t\${s2:Quoted}, s3:\t\${s3:JSON}, s4:\t\${s4:CSV}, d:\t\${d:Escaped}\t', format_schema_rows_between_delimiter = ';\n'";

$CLICKHOUSE_CLIENT --query="SELECT * FROM template ORDER BY n FORMAT CSV";
$CLICKHOUSE_CLIENT --query="DROP TABLE template";
