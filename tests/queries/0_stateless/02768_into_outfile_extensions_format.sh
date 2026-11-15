#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "
select * from numbers(1) into outfile '/dev/null';
select * from numbers(1) into outfile '/dev/null' and stdout;
select * from numbers(1) into outfile '/dev/null' append;
select * from numbers(1) into outfile '/dev/null' append and stdout;
" | ${CLICKHOUSE_FORMAT} -n
