#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "CSV"
echo 1
echo '"x","y","z"
123,"Hello","[1,2,3]"
456,"World","[4,5,6]"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
123,"Hello","[1,2,3]"
456,"World","[4,5,6]"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 2
echo '"x","y","z"
"UInt32","String","Array(UInt32)"
123,"Hello","[1,2,3]"
456,"World","[4,5,6]"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
"UInt32","String","Array(UInt32)"
123,"Hello","[1,2,3]"
456,"World","[4,5,6]"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 3
echo '"x","y","z"
"123","Hello","World"
"456","World","Hello"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
"123","Hello","World"
"456","World","Hello"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 4
echo '"x","y","z"
"UInt32","String","Array(UInt32)"
"123","Hello","World"
"456","World","Hello"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
"UInt32","String","Array(UInt32)"
"123","Hello","World"
"456","World","Hello"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 5
echo '"x","y","z"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";
echo '"x","y","z"'| $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 6
echo '"x","y","z"
"UInt32","String","Array(UInt32)"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
"UInt32","String","Array(UInt32)"'| $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 7
echo '"x","y","z"
"UInt32","String","Array"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
"UInt32","String","Array"'| $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 8
echo '"x","y","z"
"UInt32","String","Array"
"123","Hello","[1,2,3]"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
"UInt32","String","Array"
"123","Hello","[1,2,3]"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 9
echo '"x","y","z"
123,"Hello","[1,2,3]"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
123,"Hello","[1,2,3]"'| $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 10
echo '"x","y","z"
123,"""Hello""","[1,2,3]"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
123,"""Hello""","[1,2,3]"'| $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 11
echo '"x","y","z"
"Hello",\N,"World"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
"Hello",\N,"World"'| $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 12
echo '"x","y","z"
"Hello",\N,"[1,2,3]"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
"Hello",\N,"[1,2,3]"'| $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 13
echo '"x","y","z"
"Hello",\N,"World"
\N,"Hello",\N' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
"Hello",\N,"World"
\N,"Hello",\N'| $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 14
echo '"x","y","z"
"Hello",\N,\N
\N,\N,"[1,2,3]"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
"Hello",\N,\N
\N,\N,"[1,2,3]"' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 15
echo '"x","y","z"
"Hello",\N,\N
\N,"World",\N' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"x","y","z"
"Hello",\N,\N
\N,"World",\N' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 16
echo '"a""b","c"
1,2' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "desc test";

echo '"a""b","c"
1,2' | $CLICKHOUSE_LOCAL --input-format='CSV' --table='test' -q "select * from test";

echo 17
echo '"a","b","c"
1,2,3' | $CLICKHOUSE_LOCAL --input-format='CSV' --structure='a UInt32, b UInt32' --table='test' -q "select * from test";

echo 18
echo '"a"
1' | $CLICKHOUSE_LOCAL --input-format='CSV' --structure='a UInt32, b UInt32' --table='test' -q "select * from test";

echo "TSV"
echo 1
echo -e 'x\ty\tz
123\tHello\t[1,2,3]
456\tWorld\t[4,5,6]' | $CLICKHOUSE_LOCAL --input-format='TSV' --table='test' -q "desc test";

echo -e 'x\ty\tz
123\tHello\t[1,2,3]
456\tWorld\t[4,5,6]' | $CLICKHOUSE_LOCAL --input-format='TSV' --table='test' -q "select * from test";

echo 2
echo -e 'x\ty\tz
UInt32\tString\tArray(UInt32)
123\tHello\t[1,2,3]
456\tWorld\t[4,5,6]' | $CLICKHOUSE_LOCAL --input-format='TSV' --table='test' -q "desc test";

echo -e 'x\ty\tz
UInt32\tString\tArray(UInt32)
123\tHello\t[1,2,3]
456\tWorld\t[4,5,6]' | $CLICKHOUSE_LOCAL --input-format='TSV' --table='test' -q "select * from test";

echo 3
echo -e 'x\ty\tz
Foo\tHello\tWorld
Bar\tWorld\tHello' | $CLICKHOUSE_LOCAL --input-format='TSV' --table='test' -q "desc test";

echo -e 'x\ty\tz
Foo\tHello\tWorld
Bar\tWorld\tHello' | $CLICKHOUSE_LOCAL --input-format='TSV' --table='test' -q "select * from test";

echo 4
echo -e 'x\ty\tz
UInt32\tString\tArray(UInt32)
Foo\tHello\tWorld
Bar\tWorld\tHello' | $CLICKHOUSE_LOCAL --input-format='TSV' --table='test' -q "desc test";

echo -e 'x\ty\tz
UInt32\tString\tArray(UInt32)
Foo\tHello\tWorld
Bar\tWorld\tHello' | $CLICKHOUSE_LOCAL --input-format='TSV' --table='test' -q "select * from test";

echo "CustomSeparated"

echo 1
echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>123<field_delimiter>"Hello"<field_delimiter>"[1,2,3]"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>456<field_delimiter>"World"<field_delimiter>"[4,5,6]"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "desc test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'


echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>123<field_delimiter>"Hello"<field_delimiter>"[1,2,3]"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>456<field_delimiter>"World"<field_delimiter>"[4,5,6]"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "select * from test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'

echo 2
echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"UInt32"<field_delimiter>"String"<field_delimiter>"Array(UInt32)"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>123<field_delimiter>"Hello"<field_delimiter>"[1,2,3]"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>456<field_delimiter>"World"<field_delimiter>"[4,5,6]"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "desc test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'

echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"UInt32"<field_delimiter>"String"<field_delimiter>"Array(UInt32)"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>123<field_delimiter>"Hello"<field_delimiter>"[1,2,3]"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>456<field_delimiter>"World"<field_delimiter>"[4,5,6]"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "select * from test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'


echo 3
echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"Foo"<field_delimiter>"Hello"<field_delimiter>"World"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"Bar"<field_delimiter>"World"<field_delimiter>"Hello"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "desc test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'


echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"Foo"<field_delimiter>"Hello"<field_delimiter>"World"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"Bar"<field_delimiter>"World"<field_delimiter>"Hello"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "select * from test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'


echo 4
echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"UInt32"<field_delimiter>"String"<field_delimiter>"Array(UInt32)"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"Foo"<field_delimiter>"Hello"<field_delimiter>"World"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"Bar"<field_delimiter>"World"<field_delimiter>"Hello"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "desc test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'


echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"UInt32"<field_delimiter>"String"<field_delimiter>"Array(UInt32)"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"Foo"<field_delimiter>"Hello"<field_delimiter>"World"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"Bar"<field_delimiter>"World"<field_delimiter>"Hello"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "select * from test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'


echo 5
echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "desc test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'


echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "select * from test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'


echo 6
echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"UInt32"<field_delimiter>"String"<field_delimiter>"Array(UInt32)"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "desc test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'


echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"UInt32"<field_delimiter>"String"<field_delimiter>"Array(UInt32)"<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "select * from test"  --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='CSV'

echo 7
echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"UInt32"<field_delimiter>"String"<field_delimiter>"Array(UInt32)"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42<field_delimiter>"Hello"<field_delimiter>[1,2,3]<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "desc test" --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='JSON'


echo '<result_before_delimiter>
<row_before_delimiter>"x"<field_delimiter>"y"<field_delimiter>"z"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>"UInt32"<field_delimiter>"String"<field_delimiter>"Array(UInt32)"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42<field_delimiter>"Hello"<field_delimiter>[1,2,3]<row_after_delimiter>
<result_after_delimiter>' | $CLICKHOUSE_LOCAL --input-format='CustomSeparated' --table='test' -q "select * from test"  --format_custom_row_before_delimiter='<row_before_delimiter>' --format_custom_row_after_delimiter=$'<row_after_delimiter>\n' --format_custom_row_between_delimiter=$'<row_between_delimiter>\n' --format_custom_result_before_delimiter=$'<result_before_delimiter>\n' --format_custom_result_after_delimiter=$'<result_after_delimiter>\n' --format_custom_field_delimiter='<field_delimiter>' --format_custom_escaping_rule='JSON'


