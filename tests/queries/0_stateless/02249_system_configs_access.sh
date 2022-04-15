#!/usr/bin/env bash
# Tags: no-parallel, use-simdjson
# Tag no-parallel: create user

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS user_test_02249;"
$CLICKHOUSE_CLIENT --query "CREATE USER user_test_02249 IDENTIFIED WITH plaintext_password BY 'user_test_02249';"

${CLICKHOUSE_CLIENT} -q "REVOKE ALL ON *.* FROM user_test_02249"
$CLICKHOUSE_CLIENT  --user=user_test_02249 --password=user_test_02249 --query "select * from system.configs" 2>&1 | grep -Fo "ACCESS_DENIED" | uniq

$CLICKHOUSE_CLIENT --query "GRANT SYSTEM CONFIGS ON *.* TO user_test_02249;"
$CLICKHOUSE_CLIENT  --user=user_test_02249 --password=user_test_02249 --query "select JSON_QUERY(config, '$.http_port'), JSON_QUERY(config, '$.tcp_port') from system.configs" 
