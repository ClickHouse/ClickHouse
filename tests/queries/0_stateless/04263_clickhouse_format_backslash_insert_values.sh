#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "insert into t values (1, 'hello'), (2, 'world')" | ${CLICKHOUSE_FORMAT} --backslash
echo "insert into t values (1, 'hello'), (2, 'world')" | ${CLICKHOUSE_FORMAT} --backslash --multiquery
