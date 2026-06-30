#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/108956
# The ON CLUSTER form of SYSTEM PREWARM PRIMARY INDEX CACHE used to require
# SYSTEM PREWARM MARK CACHE (copy-paste bug in getRequiredAccessForDDLOnCluster).
# A user granted only SYSTEM PREWARM PRIMARY INDEX CACHE must be able to run the
# ON CLUSTER form, matching the local-execution access check.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
table="t_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -mq "
        DROP USER IF EXISTS $user;
        DROP TABLE IF EXISTS $table;
    "
}
cleanup
trap cleanup EXIT

$CLICKHOUSE_CLIENT -mq "
    CREATE TABLE $table (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS prewarm_primary_key_cache = 1, use_primary_key_cache = 1;
    INSERT INTO $table SELECT number FROM numbers(100);

    CREATE USER $user;
    -- Grant ONLY the primary-index-cache prewarm privilege (and CLUSTER), NOT mark-cache.
    -- SYSTEM PREWARM PRIMARY INDEX CACHE is a GLOBAL privilege (granted ON *.*).
    GRANT CLUSTER, SYSTEM PREWARM PRIMARY INDEX CACHE ON *.* TO $user;
"

# Local form: already works (checks SYSTEM_PREWARM_PRIMARY_INDEX_CACHE).
# Assert the command actually succeeds (zero exit), not just the absence of ACCESS_DENIED.
echo "local"
$CLICKHOUSE_CLIENT --user "$user" -q "SYSTEM PREWARM PRIMARY INDEX CACHE $CLICKHOUSE_DATABASE.$table" >/dev/null || exit 1
echo "ok"

# ON CLUSTER form: before the fix it was denied asking for SYSTEM PREWARM MARK CACHE.
echo "on cluster"
$CLICKHOUSE_CLIENT --user "$user" -q "SYSTEM PREWARM PRIMARY INDEX CACHE ON CLUSTER test_shard_localhost $CLICKHOUSE_DATABASE.$table" >/dev/null || exit 1
echo "ok"

# Negative control: a user WITHOUT the privilege is denied, and the required grant
# named in the error is SYSTEM PREWARM PRIMARY INDEX CACHE (not MARK CACHE).
echo "denied without grant"
$CLICKHOUSE_CLIENT -q "REVOKE SYSTEM PREWARM PRIMARY INDEX CACHE ON *.* FROM $user"
$CLICKHOUSE_CLIENT --user "$user" -q "SYSTEM PREWARM PRIMARY INDEX CACHE ON CLUSTER test_shard_localhost $CLICKHOUSE_DATABASE.$table" 2>&1 \
    | grep -m1 -F -o "the grant SYSTEM PREWARM PRIMARY INDEX CACHE" || echo "wrong error"
