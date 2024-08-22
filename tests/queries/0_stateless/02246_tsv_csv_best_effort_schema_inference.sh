#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FILE_NAME=test_02149.data
DATA_FILE=${USER_FILES_PATH:?}/$FILE_NAME

touch $DATA_FILE

echo "TSV"

echo -e "42\tSome string\t[1, 2, 3, 4]\t(1, 2, 3)
42\tabcd\t[]\t(4, 5, 6)" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "[({'key' : 42.42}, ['String', 'String2'], 42.42), ({}, [], -42), ({'key2' : NULL}, [NULL], NULL)]
[]
[({}, [], 0)]
[({}, [NULL], NULL)]
[({}, ['String3'], NULL)]
[({'key3': NULL}, []), NULL]"> $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV') settings input_format_tsv_use_best_effort_in_schema_inference=false"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV') settings input_format_tsv_use_best_effort_in_schema_inference=false"


echo -e "[({'key' : 42.42}, ['String', 'String2'], 42.42), ({}, [], -42), ({'key2' : NULL}, [NULL], NULL)]
[]
[({}, [], 0)]
[({}, [NULL], NULL)]
[({}, ['String3'], NULL)]
[({'key3': NULL}, [], NULL)]"> $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "true
false
\N" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "[true, NULL]
[]
[NULL]
[false]" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "[]" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "{}" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "()" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "[1, 2, 3" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "[(1, 2, 3 4)]" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "[1, 2, 3 + 4]" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "(1, 2," > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "[1, Some trash, 42.2]" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "[1, 'String', {'key' : 2}]" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "{'key' : 1, [1] : 10}" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "{}{}" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "[1, 2, 3" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "[abc, def]" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "['abc', 'def']" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "['string]" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "'string" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "42.42" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "42.42sometrash" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"

echo -e "[42.42sometrash, 42.42]" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'TSV')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'TSV')"


echo
echo "CSV"

echo -e "42,Some string,'[1, 2, 3, 4]','[(1, 2, 3)]'
42\,abcd,'[]','[(4, 5, 6)]'" > $DATA_FILE

CLIENT_CMD="$CLICKHOUSE_CLIENT --format_csv_allow_single_quotes=1"

$CLIENT_CMD -q "desc file('$FILE_NAME', 'CSV')"
$CLIENT_CMD -q "select * from file('$FILE_NAME', 'CSV')"

echo -e "\"[({'key' : 42.42}, ['String', 'String2'], 42.42), ({}, [], -42), ({'key2' : NULL}, [NULL], NULL)]\"
'[]'
'[({}, [], 0)]'
'[({}, [NULL], NULL)]'
\"[({}, ['String3'], NULL)]\"
\"[({'key3': NULL}, []), NULL]\""> $DATA_FILE

$CLIENT_CMD -q "desc file('$FILE_NAME', 'CSV') settings input_format_csv_use_best_effort_in_schema_inference=false"
$CLIENT_CMD -q "select * from file('$FILE_NAME', 'CSV') settings input_format_csv_use_best_effort_in_schema_inference=false"

echo -e "\"[({'key' : 42.42}, ['String', 'String2'], 42.42), ({}, [], -42), ({'key2' : NULL}, [NULL], NULL)]\"
'[]'
'[({}, [], 0)]'
'[({}, [NULL], NULL)]'
\"[({}, ['String3'], NULL)]\"
\"[({'key3': NULL}, [], NULL)]\""> $DATA_FILE

$CLIENT_CMD -q "desc file('$FILE_NAME', 'CSV')"
$CLIENT_CMD -q "select * from file('$FILE_NAME', 'CSV')"

echo -e "true
false
\N" > $DATA_FILE

$CLIENT_CMD -q "desc file('$FILE_NAME', 'CSV')"
$CLIENT_CMD -q "select * from file('$FILE_NAME', 'CSV')"

echo -e "'[true, NULL]'
'[]'
'[NULL]'
'[false]'" > $DATA_FILE

$CLIENT_CMD -q "desc file('$FILE_NAME', 'CSV')"
$CLIENT_CMD -q "select * from file('$FILE_NAME', 'CSV')"


echo -e "'(1, 2, 3)'"> $DATA_FILE

$CLIENT_CMD -q "desc file('$FILE_NAME', 'CSV')"
$CLIENT_CMD -q "select * from file('$FILE_NAME', 'CSV')"

echo -e '"123.123"'> $DATA_FILE

$CLIENT_CMD -q "desc file('$FILE_NAME', 'CSV')"
$CLIENT_CMD -q "select * from file('$FILE_NAME', 'CSV')"

echo -e "'[(1, 2, 3)]'"> $DATA_FILE

$CLIENT_CMD -q "desc file('$FILE_NAME', 'CSV')"
$CLIENT_CMD -q "select * from file('$FILE_NAME', 'CSV')"

echo -e "\"[(1, 2, 3)]\""> $DATA_FILE

$CLIENT_CMD -q "desc file('$FILE_NAME', 'CSV')"
$CLIENT_CMD -q "select * from file('$FILE_NAME', 'CSV')"
