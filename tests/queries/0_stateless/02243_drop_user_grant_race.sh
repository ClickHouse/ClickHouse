#!/usr/bin/env bash
# Tags: race, no-fasttest, no-parallel

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    DROP ROLE IF EXISTS test_role_02244;
    CREATE ROLE test_role_02244;
    DROP USER IF EXISTS kek_02243;
    CREATE USER IF NOT EXISTS kek_02243;
    REVOKE ALL ON *.* FROM kek_02243;
    CREATE TABLE test (n int) engine=Memory;
    INSERT INTO test VALUES (1);
"

function create_drop_grant()
{
    while true; do
        $CLICKHOUSE_CLIENT -q "CREATE USER IF NOT EXISTS test_user_02243 GRANTEES NONE" ||:
        $CLICKHOUSE_CLIENT -q "GRANT ALL ON *.* TO test_user_02243 WITH GRANT OPTION" ||:
        $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_user_02243" &
        $CLICKHOUSE_CLIENT --user test_user_02243 -q "GRANT ALL ON *.* TO kek_02243" &
    done
}

export -f create_drop_grant

TIMEOUT=10
timeout $TIMEOUT bash -c create_drop_grant 2> /dev/null &
wait

$CLICKHOUSE_CLIENT --user kek_02243 -q "SELECT * FROM test" 2>&1| grep -Fa "Exception: " | grep -Eo ACCESS_DENIED | uniq

$CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS test_role_02243"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_user_02243"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS kek_02243"
