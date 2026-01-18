#!/usr/bin/env bash
# Tags: long, no-asan, no-tsan, no-ubsan, no-msan
# Too slow for sanitizers

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --enable_json_type=1 -q 'drop table if exists test_json; create table test_json(data JSON) engine = MergeTree order by tuple()';

echo '{"a" : 4, "b" : 5 }' | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&max_block_size=1&async_insert=1&wait_for_async_insert=0&async_insert_max_data_size=100000000&query=insert+into+test_json+format+TSV" --data-binary @-;

python3 -c "import json; [print(json.dumps({'z' : i, 'b' : '+'})) for i in range(2000000)];print('{\"z\" : 42, "b" : 4ohno }')"  | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&max_block_size=1&async_insert=1&wait_for_async_insert=0&async_insert_max_data_size=100000000&query=insert+into+test_json+format+TSV" --data-binary @- ;

echo '{"z" : "world", "b" : ":)", "y" : 5 }' | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&max_block_size=1&async_insert=1&wait_for_async_insert=0&async_insert_max_data_size=100000000&query=insert+into+test_json+format+TSV" --data-binary @-;

$CLICKHOUSE_CLIENT -q "drop table test_json";
