#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: `ATTACH TABLE` with a full definition is a local metadata entry point, not replicated DDL.

# `leader_election` is implemented only for `MergeTree`, and setting it on `ReplicatedMergeTree` is rejected
# both at `CREATE TABLE` and at `ATTACH TABLE`, which is a separate metadata-driven entry point.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table_uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# An `Atomic` database accepts `ATTACH TABLE` with a full definition only together with a `UUID`.
for query in "CREATE TABLE t_leader_election_replicated" "ATTACH TABLE t_leader_election_replicated UUID '$table_uuid'"
do
    $CLICKHOUSE_CLIENT -q "
        $query (x UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_leader_election_replicated', 'r1')
        ORDER BY x
        SETTINGS leader_election = 1
    " 2>&1 | grep -o 'The `leader_election` setting is only supported on the `MergeTree` engine' | head -n 1
done

$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_leader_election_replicated"
