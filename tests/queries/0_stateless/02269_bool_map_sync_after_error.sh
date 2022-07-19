#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (x Bool, y Map(String, String)) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --input_format_allow_errors_num 1 --input_format_allow_errors_ratio 1 --query "
INSERT INTO test FORMAT TSV
true	{'Hello': 'world1'}"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test ORDER BY x, y"

${CLICKHOUSE_CLIENT} --input_format_allow_errors_num 1 --input_format_allow_errors_ratio 1 --query "
INSERT INTO test FORMAT TSV
ture	{'Hello': 'world2'}
true	{'Hello': 'world3'}"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test ORDER BY x, y"

${CLICKHOUSE_CLIENT} --input_format_allow_errors_num 1 --input_format_allow_errors_ratio 1 --query "
INSERT INTO test FORMAT TSV
2	{'Hello': 'world4'}
true	{'Hello': 'world5'}"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test ORDER BY x, y"

${CLICKHOUSE_CLIENT} --input_format_allow_errors_num 1 --input_format_allow_errors_ratio 1 --query "
INSERT INTO test FORMAT TSV
true	{'Hello': 'world6': 'goodbye'}
true	{'Hello': 'world7'}"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test ORDER BY x, y"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test"
