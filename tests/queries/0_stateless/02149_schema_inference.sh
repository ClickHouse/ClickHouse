#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME=test_02149.data
DATA_FILE=${USER_FILES_PATH:?}/$FILE_NAME

touch $DATA_FILE

SCHEMADIR=$(clickhouse-client --query "select * from file('$FILE_NAME', 'Template', 'val1 char') settings format_template_row='nonexist'" 2>&1 | grep Exception | grep -oP "file \K.*(?=/nonexist)")

echo "TSV"

echo -e "42\tSome string\t[1, 2, 3, 4]\t(1, 2, 3)
42\tabcd\t[]\t(4, 5, 6)" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo "TSVWithNames"

echo -e "number\tstring\tarray\ttuple
42\tSome string\t[1, 2, 3, 4]\t(1, 2, 3)
42\tabcd\t[]\t(4, 5, 6)" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSVWithNames')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSVWithNames')"

echo "CSV"

echo -e "\N,\"Some string\",\"[([1, 2.3], 'String'), ([], NULL)]\",\"[1, NULL, 3]\"
42,\N,\"[([1, 2.3], 'String'), ([3.], 'abcd')]\",\"[4, 5, 6]\"" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'CSV')"

echo -e "42,\"String\"
\"String\",42" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'CSV')"

echo -e "\N,\"[NULL, NULL]\"
\N,[]" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'CSV')"

echo "CSVWithNames"

echo -e "a,b,c,d
\N,\"Some string\",\"[([1, 2.3], 'String'), ([], NULL)]\",\"[1, NULL, 3]\"
42,\N,\"[([1, 2.3], 'String'), ([3.], 'abcd')]\",\"[4, 5, 6]\"" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CSVWithNames')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'CSVWithNames')"

echo "JSONCompactEachRow"

echo -e "[42.42, [[1, \"String\"], [2, \"abcd\"]], {\"key\" : 42, \"key2\" : 24}, true]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONCompactEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONCompactEachRow')"

echo -e "[null, [[1, \"String\"], [2, null]], {\"key\" : null, \"key2\" : 24}, null]
[32, [[2, \"String 2\"], [3, \"hello\"]], {\"key3\" : 4242, \"key4\" : 2424}, true]"  > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONCompactEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONCompactEachRow')"

echo "JSONCompactEachRowWithNames"

echo -e "[\"a\", \"b\", \"c\", \"d\"]
[42.42, [[1, \"String\"], [2, \"abcd\"]], {\"key\" : 42, \"key2\" : 24}, true]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONCompactEachRowWithNames')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONCompactEachRowWithNames')"


echo "JSONEachRow"
echo -e '{"a" : 42.42, "b" : [[1, "String"], [2, "abcd"]], "c" : {"key" : 42, "key2" : 24}, "d" : true}' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONEachRow')"

echo -e '{"a" : null, "b" : [[1, "String"], [2, null]], "c" : {"key" : null, "key2" : 24}, "d" : null}
{"a" : 32, "b" : [[2, "String 2"], [3, "hello"]], "c" : {"key3" : 4242, "key4" : 2424}, "d" : true}'  > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONEachRow')"

echo -e '{"a" : 1, "b" : "s1", "c" : null}
{"c" : [2], "a" : 2, "b" : null}
{}
{"a" : null}
{"c" : [3], "a" : null}'  > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'JSONEachRow')"


echo "TSKV"

echo -e 'a=1\tb=s1\tc=\N
c=[2]\ta=2\tb=\N}

a=\N
c=[3]\ta=\N'  > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSKV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSKV')"


echo "Values"

echo -e "(42.42, 'Some string', [1, 2, 3], (1, '2'), ([1, 2], [(3, '4'), (5, '6')]))" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Values')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Values')"

echo -e "(42.42, NULL, [1, NULL, 3], (1, NULL), ([1, 2], [(3, '4'), (5, '6')])), (NULL, 'Some string', [10], (1, 2), ([], []))" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Values')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Values')"


echo "Regexp"

REGEXP="^Line: value_1=(.+?), value_2=(.+?), value_3=(.+?)"

echo "Line: value_1=42, value_2=Some string 1, value_3=[([1, 2, 3], String 1), ([], String 1)]
Line: value_1=2, value_2=Some string 2, value_3=[([4, 5, 6], String 2), ([], String 2)]
Line: value_1=312, value_2=Some string 3, value_3=[([1, 2, 3], String 2), ([], String 2)]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Regexp') settings format_regexp='$REGEXP', format_regexp_escaping_rule='Escaped'"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Regexp') settings format_regexp='$REGEXP', format_regexp_escaping_rule='Escaped'"


echo "Line: value_1=42, value_2=\"Some string 1\", value_3=\"[([1, 2, 3], 'String 1'), ([], 'String 1')]\"
Line: value_1=3, value_2=\"Some string 2\", value_3=\"[([3, 5, 1], 'String 2'), ([], 'String 2')]\"
Line: value_1=244, value_2=\"Some string 3\", value_3=\"[([], 'String 3'), ([], 'String 3')]\"" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Regexp') settings format_regexp='$REGEXP', format_regexp_escaping_rule='CSV'"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Regexp') settings format_regexp='$REGEXP', format_regexp_escaping_rule='CSV'"


echo "Line: value_1=42, value_2='Some string 1', value_3=[([1, 2, 3], 'String 1'), ([], 'String 1')]
Line: value_1=2, value_2='Some string 2', value_3=[([], 'String 2'), ([], 'String 2')]
Line: value_1=43, value_2='Some string 3', value_3=[([1, 5, 3], 'String 3'), ([], 'String 3')]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Regexp') settings format_regexp='$REGEXP', format_regexp_escaping_rule='Quoted'"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Regexp') settings format_regexp='$REGEXP', format_regexp_escaping_rule='Quoted'"


echo "Line: value_1=42, value_2=\"Some string 1\", value_3=[[[1, 2, 3], \"String 1\"], [[1], \"String 1\"]]
Line: value_1=52, value_2=\"Some string 2\", value_3=[[[], \"String 2\"], [[1], \"String 2\"]]
Line: value_1=24, value_2=\"Some string 3\", value_3=[[[1, 2, 3], \"String 3\"], [[1], \"String 3\"]]" > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Regexp') settings format_regexp='$REGEXP', format_regexp_escaping_rule='JSON'"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Regexp') settings format_regexp='$REGEXP', format_regexp_escaping_rule='JSON'"


echo "CustomSeparated"

CUSTOM_SETTINGS="SETTINGS format_custom_row_before_delimiter='<row_before_delimiter>', format_custom_row_after_delimiter='<row_after_delimiter>\n', format_custom_row_between_delimiter='<row_between_delimiter>\n', format_custom_result_before_delimiter='<result_before_delimiter>\n', format_custom_result_after_delimiter='<result_after_delimiter>\n', format_custom_field_delimiter='<field_delimiter>'"

echo -e "<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter>\"Some string 1\"<field_delimiter>\"[([1, 2, 3], 'String 1'), ([1], 'String 1')]\"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42<field_delimiter>\"Some string 2\"<field_delimiter>\"[([], 'String 2'), ([], 'String 2')]\"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>\N<field_delimiter>\"Some string 3\"<field_delimiter>\"[([1, 2, 3], 'String 3'), ([1], 'String 3')]\"<row_after_delimiter>
<result_after_delimiter>" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CustomSeparated') $CUSTOM_SETTINGS, format_custom_escaping_rule='CSV'"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'CustomSeparated') $CUSTOM_SETTINGS, format_custom_escaping_rule='CSV'"

echo -e "<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter>'Some string 1'<field_delimiter>[([1, 2, 3], 'String 1'), ([1], 'String 1')]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42<field_delimiter>'Some string 2'<field_delimiter>[([], 'String 2'), ([], 'String 2')]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>NULL<field_delimiter>'Some string 3'<field_delimiter>[([1, 2, 3], 'String 3'), ([1], 'String 3')]<row_after_delimiter>
<result_after_delimiter>" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CustomSeparated') $CUSTOM_SETTINGS, format_custom_escaping_rule='Quoted'"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'CustomSeparated') $CUSTOM_SETTINGS, format_custom_escaping_rule='Quoted'"

echo -e "<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter>\"Some string 1\"<field_delimiter>[[[1, 2, 3], \"String 1\"], [[1], \"String 1\"]]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42<field_delimiter>\"Some string 2\"<field_delimiter>[[[], \"String 2\"], [[], \"String 2\"]]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>null<field_delimiter>\"Some string 3\"<field_delimiter>[[[1, 2, 3], \"String 3\"], [[1], \"String 3\"]]<row_after_delimiter>
<result_after_delimiter>" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CustomSeparated') $CUSTOM_SETTINGS, format_custom_escaping_rule='JSON'"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'CustomSeparated') $CUSTOM_SETTINGS, format_custom_escaping_rule='JSON'"


echo "Template"

echo -e "<result_before_delimiter>
\${data}<result_after_delimiter>" > $SCHEMADIR/resultset_format_02149

echo -e "<row_before_delimiter>\${column_1:CSV}<field_delimiter_1>\${column_2:CSV}<field_delimiter_2>\${column_3:CSV}<row_after_delimiter>" > $SCHEMADIR/row_format_02149

TEMPLATE_SETTINGS="SETTINGS format_template_rows_between_delimiter='<row_between_delimiter>\n', format_template_row='row_format_02149', format_template_resultset='resultset_format_02149'"

echo -e "<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter_1>\"Some string 1\"<field_delimiter_2>\"[([1, 2, 3], 'String 1'), ([1], 'String 1')]\"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42<field_delimiter_1>\"Some string 2\"<field_delimiter_2>\"[([], 'String 2'), ([], 'String 2')]\"<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>\N<field_delimiter_1>\"Some string 3\"<field_delimiter_2>\"[([1, 2, 3], 'String 3'), ([1], 'String 3')]\"<row_after_delimiter>
<result_after_delimiter>" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Template') $TEMPLATE_SETTINGS"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Template') $TEMPLATE_SETTINGS"

echo -e "<row_before_delimiter>\${column_1:Quoted}<field_delimiter_1>\${column_2:Quoted}<field_delimiter_2>\${column_3:Quoted}<row_after_delimiter>" > $SCHEMADIR/row_format_02149

echo -e "<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter_1>'Some string 1'<field_delimiter_2>[([1, 2, 3], 'String 1'), ([1], 'String 1')]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42<field_delimiter_1>'Some string 2'<field_delimiter_2>[([], 'String 2'), ([], 'String 2')]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>NULL<field_delimiter_1>'Some string 3'<field_delimiter_2>[([1, 2, 3], 'String 3'), ([1], 'String 3')]<row_after_delimiter>
<result_after_delimiter>" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Template') $TEMPLATE_SETTINGS"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Template') $TEMPLATE_SETTINGS"

echo -e "<row_before_delimiter>\${column_1:JSON}<field_delimiter_1>\${column_2:JSON}<field_delimiter_2>\${column_3:JSON}<row_after_delimiter>" > $SCHEMADIR/row_format_02149

echo -e "<result_before_delimiter>
<row_before_delimiter>42.42<field_delimiter_1>\"Some string 1\"<field_delimiter_2>[[[1, 2, 3], \"String 1\"], [[1], \"String 1\"]]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>42<field_delimiter_1>\"Some string 2\"<field_delimiter_2>[[[], \"String 2\"], [[], \"String 2\"]]<row_after_delimiter>
<row_between_delimiter>
<row_before_delimiter>null<field_delimiter_1>\"Some string 3\"<field_delimiter_2>[[[1, 2, 3], \"String 3\"], [[1], \"String 3\"]]<row_after_delimiter>
<result_after_delimiter>" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Template') $TEMPLATE_SETTINGS"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Template') $TEMPLATE_SETTINGS"


echo "MsgPack"

$CLICKHOUSE_CLIENT -q "select toInt32(number % 2 ? number : NULL) as int, toUInt64(number % 2 ? NULL : number) as uint, toFloat32(number) as float, concat('Str: ', toString(number)) as str, [[number, number + 1], [number]] as arr, map(number, [number, number + 1]) as map from numbers(3) format MsgPack" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'MsgPack') settings input_format_msgpack_number_of_columns=6"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'MsgPack') settings input_format_msgpack_number_of_columns=6"


rm $SCHEMADIR/resultset_format_02149 $SCHEMADIR/row_format_02149
rm $DATA_FILE

