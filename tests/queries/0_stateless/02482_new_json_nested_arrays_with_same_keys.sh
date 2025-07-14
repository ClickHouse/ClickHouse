#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

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
}' > 02482_object_data.jsonl

$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 -q "select * from file(02482_object_data.jsonl, auto, 'obj JSON')"

rm 02482_object_data.jsonl

