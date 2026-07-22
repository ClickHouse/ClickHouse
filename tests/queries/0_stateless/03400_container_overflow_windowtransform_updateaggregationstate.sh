#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# We just want to ensure the following query doesn't create an ASAN container-overflow
# https://github.com/ClickHouse/ClickHouse/issues/75660
res=$($CLICKHOUSE_CLIENT --query """
SET enable_json_type = 1;
CREATE TABLE test (\`id\` UInt64, \`json\` JSON(max_dynamic_paths = 2, \`a.b.c\` UInt32))
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO test SELECT number, '{}' FROM numbers(5);
INSERT INTO test SELECT number, toJSONString(map('a.b.c', number)) FROM numbers(5, 5);
INSERT INTO test SELECT number, toJSONString(map('a.b.d', CAST(number, 'UInt32'), 'a.b.e', concat('str_', toString(number)))) FROM numbers(10, 5); -- { serverError NO_COMMON_TYPE }
INSERT INTO test SELECT number, toJSONString(map('b.b.d', CAST(number, 'UInt32'), 'b.b.e', concat('str_', toString(number)))) FROM numbers(15, 5); -- { serverError NO_COMMON_TYPE }
INSERT INTO test SELECT number, toJSONString(map('a.b.c', number, 'a.b.d', CAST(number, 'UInt32'), 'a.b.e', concat('str_', toString(number)))) FROM numbers(20, 5); -- { serverError NO_COMMON_TYPE }
INSERT INTO test SELECT number, toJSONString(map('a.b.c', number, 'a.b.d', CAST(number, 'UInt32'), 'a.b.e', concat('str_', toString(number)), concat('b.b._', toString(number)), CAST(number, 'UInt32'))) FROM numbers(25, 5); -- { serverError NO_COMMON_TYPE }
INSERT INTO test SELECT number, toJSONString(map('a.b.c', number, 'a.b.d', CAST(range(number % 1), 'Array(UInt32)'), 'a.b.e', concat('str_', toString(number)), 'd.a', CAST(number, 'UInt32'), 'd.c', toDate(number))) FROM numbers(30, 5); -- { serverError NO_COMMON_TYPE }
INSERT INTO test SELECT number, toJSONString(map('a.b.c', number, 'a.b.d', toDateTime(number), 'a.b.e', concat('str_', toString(number)), 'd.a', CAST(range((number % 5) + 1), 'Array(UInt32)'), 'd.b', CAST(number, 'UInt32'))) FROM numbers(35, 5); -- { serverError NO_COMMON_TYPE }

SELECT count(65536) IGNORE NULLS OVER (ORDER BY json.b.b._28.:Int64 DESC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING), json.b.b.e.:String
FROM test
ORDER BY id DESC NULLS LAST
FORMAT JSONColumns;
""")

echo "OK"
