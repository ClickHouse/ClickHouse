#!/usr/bin/env bash
# Tags: no-replicated-database, no-async-insert

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user03580_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} --query "CREATE USER $user IDENTIFIED WITH no_authentication";
(( $(${CLICKHOUSE_CLIENT} --user $user --query "SELECT 1" 2>&1 | grep -c "Authentication failed") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --query "DROP USER $user";
