#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo '
{
    "obj" :
    {
        "list" :
        [
            {
                "nested" : {
                    "x" : [{"r" : 1}, {"r" : 2}]
                },
                "x" : [{"r" : 1}]
            }
        ]
    }
}' > $CLICKHOUSE_TEST_UNIQUE_NAME.jsonl

$CLICKHOUSE_LOCAL --enable_json_type=1 -q "select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.jsonl', auto, 'obj JSON')"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.jsonl
