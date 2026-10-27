#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the successful `SYSTEM DISABLE ALL FAILPOINTS` below disarms every fail point on
# the server, which would break a concurrently running test that has one enabled.

# `SYSTEM DISABLE ALL FAILPOINTS` is guarded by the `SYSTEM FAILPOINT` privilege, like every
# other `SYSTEM ... FAILPOINT` statement. It takes no name, so it cannot fall back on the
# per-name check - the access check for this branch has to be pinned on its own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user"
$CLICKHOUSE_CLIENT -q "CREATE USER $user"

echo "-- without SYSTEM FAILPOINT"
$CLICKHOUSE_CLIENT --user "$user" -q "SYSTEM DISABLE ALL FAILPOINTS" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "FAIL: statement was allowed without SYSTEM FAILPOINT"

echo "-- with SYSTEM FAILPOINT"
$CLICKHOUSE_CLIENT -q "GRANT SYSTEM FAILPOINT ON *.* TO $user"
$CLICKHOUSE_CLIENT --user "$user" -q "SYSTEM DISABLE ALL FAILPOINTS" && echo "OK"

# An unrelated `SYSTEM` privilege is not enough - the statement is not covered by a parent grant
# broader than `SYSTEM FAILPOINT`, except `SYSTEM` and `ALL` themselves.
echo "-- with another SYSTEM privilege only"
$CLICKHOUSE_CLIENT -q "REVOKE SYSTEM FAILPOINT ON *.* FROM $user"
$CLICKHOUSE_CLIENT -q "GRANT SYSTEM DROP CACHE ON *.* TO $user"
$CLICKHOUSE_CLIENT --user "$user" -q "SYSTEM DISABLE ALL FAILPOINTS" 2>&1 | grep -m1 -o "ACCESS_DENIED" || echo "FAIL: statement was allowed with an unrelated SYSTEM privilege"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
