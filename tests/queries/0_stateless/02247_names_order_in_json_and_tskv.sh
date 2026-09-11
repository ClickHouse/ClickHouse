#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FILE_NAME=test_02247.data
DATA_FILE=${CLICKHOUSE_USER_FILES_UNIQUE:?}/$FILE_NAME

touch $DATA_FILE

echo -e 'a=1\tb=s1\tc=\N
c=[2]\ta=2\tb=\N}
a=\N

c=[3]\ta=\N' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/$FILE_NAME', 'TSKV')"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/$FILE_NAME', 'TSKV')"

echo -e 'b=1
a=2\tc=3

e=3
c=1\tb=3\ta=3' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/$FILE_NAME', 'TSKV')"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/$FILE_NAME', 'TSKV')"


echo -e '{"a" : 1, "b" : "s1", "c" : null}
{"c" : [2], "a" : 2, "b" : null}
{}
{"a" : null}
{"c" : [3], "a" : null}' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/$FILE_NAME', 'JSONEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/$FILE_NAME', 'JSONEachRow')"

echo -e '{"b" : 1}
{"a" : 2, "c" : 3}
{}
{"e" : 3}
{"c" : 1, "b" : 3, "a" : 3}' > $DATA_FILE
$CLICKHOUSE_CLIENT -q "desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/$FILE_NAME', 'JSONEachRow')"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/$FILE_NAME', 'JSONEachRow')"


rm $DATA_FILE
