#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS user_test_02184;"
$CLICKHOUSE_CLIENT --query "CREATE USER user_test_02184 IDENTIFIED WITH plaintext_password BY 'user_test_02184';"
${CLICKHOUSE_CLIENT} -q "REVOKE ALL ON *.* FROM user_test_02184"

$CLICKHOUSE_CLIENT --query "GRANT CREATE ON *.* TO user_test_02184;"

$CLICKHOUSE_CLIENT --query "CREATE TABLE url ENGINE=URL('https://clickhouse.com', LineAsString)"

$CLICKHOUSE_CLIENT  --user=user_test_02184 --password=user_test_02184  --query "CREATE TABLE t AS url" 2>&1| grep -Fo "ACCESS_DENIED" | uniq

$CLICKHOUSE_CLIENT --query "GRANT URL ON *.* TO user_test_02184;"
$CLICKHOUSE_CLIENT --user=user_test_02184 --password=user_test_02184  --query "CREATE TABLE t AS url"
$CLICKHOUSE_CLIENT --query "SHOW CREATE TABLE t"
$CLICKHOUSE_CLIENT --query "DROP TABLE t"
$CLICKHOUSE_CLIENT --query "DROP TABLE url"
