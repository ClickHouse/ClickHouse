#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest

# Regression test for a data race in ZooKeeper client between sendThread and receiveThread.
#
# sendThread used to mutate the request (addRootPath, has_watch) AFTER copying it
# into the operations map, while receiveThread could concurrently read from the
# same shared request object via the operations map. This caused a data race on
# the request's path string (std::string reallocation during addRootPath vs
# concurrent getPath() read), leading to SIGBUS/use-after-free crashes.
#
# Under TSAN this test reliably detects the race before the fix.
# The key is to generate many concurrent ZooKeeper requests through the server's
# shared ZK session so sendThread and receiveThread are both actively working on
# the operations map at the same time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_zk_race;
    CREATE TABLE t_zk_race (key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_zk_race', 'r1')
    ORDER BY key;
"

ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_zk_race"

# Flood the server's shared ZK connection with concurrent reads from
# system.zookeeper. Each SELECT issues ZK list/get requests that go through
# sendThread (addRootPath + operations map insert) and receiveThread
# (operations map read for timeout + response handling) on the same session.
#
# Use clickhouse-benchmark for maximum ZK operations/sec on a single session.
# --timelimit ensures the test runs long enough for TSAN to catch the race.
echo "SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH' FORMAT Null" | \
    ${CLICKHOUSE_BENCHMARK} --concurrency 30 --iterations 100000 --timelimit 10 2>&1 | grep -q "Executed" || true

echo "OK"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_zk_race"
