#!/usr/bin/env bash
# Tags: no-parallel


CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT_BINARY}  --query "drop user if exists u_02311"
${CLICKHOUSE_CLIENT_BINARY}  --query "create user u_02311"
error="$(${CLICKHOUSE_CLIENT_BINARY}  --user=u_02311 --query "insert into system.zookeeper (path, name, value) values ('//3-insert-testc/c/c/kk', 'kk', '11')" 2>&1 > /dev/null)"
echo "${error}" | grep -Fc "ACCESS_DENIED"

${CLICKHOUSE_CLIENT_BINARY}  --query "drop user u_02311"
