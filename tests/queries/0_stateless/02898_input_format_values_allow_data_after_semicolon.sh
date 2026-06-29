#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "insert into function null() values (1); -- { foo }"
$CLICKHOUSE_LOCAL  -q "insert into function null() values (1); -- { foo }"
