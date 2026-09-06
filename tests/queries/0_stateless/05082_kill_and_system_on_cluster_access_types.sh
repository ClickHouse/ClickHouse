#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Holds CLUSTER and nothing else.
cluster_user="cluster_$CLICKHOUSE_TEST_UNIQUE_NAME"
# Holds every statement privilege but not CLUSTER.
no_cluster_user="no_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME"
# Hold the complete privilege set of one statement each.
kill_txn_user="kill_txn_$CLICKHOUSE_TEST_UNIQUE_NAME"
move_user="move_$CLICKHOUSE_TEST_UNIQUE_NAME"
# Hold all but one privilege of one statement each.
partial_txn_user="partial_txn_$CLICKHOUSE_TEST_UNIQUE_NAME"
partial_move_user="partial_move_$CLICKHOUSE_TEST_UNIQUE_NAME"

function cleanup()
{
    $CLICKHOUSE_CLIENT -mq "
        DROP USER IF EXISTS $cluster_user;
        DROP USER IF EXISTS $no_cluster_user;
        DROP USER IF EXISTS $kill_txn_user;
        DROP USER IF EXISTS $move_user;
        DROP USER IF EXISTS $partial_txn_user;
        DROP USER IF EXISTS $partial_move_user;
    "
}
cleanup
trap cleanup EXIT

# CLUSTER is a global privilege, so it cannot share a GRANT with a table-scoped one.
$CLICKHOUSE_CLIENT -mq "
    CREATE USER $cluster_user, $no_cluster_user, $kill_txn_user, $move_user,
                $partial_txn_user, $partial_move_user IDENTIFIED WITH no_password;

    GRANT CLUSTER ON *.* TO $cluster_user;

    GRANT KILL TRANSACTION, SYSTEM THREAD FUZZER, SYSTEM ON *.* TO $no_cluster_user;

    GRANT CLUSTER, KILL TRANSACTION ON *.* TO $kill_txn_user;
    GRANT SELECT ON system.transactions TO $kill_txn_user;

    GRANT CLUSTER, ALTER MOVE PARTITION, MOVE PARTITION BETWEEN SHARDS ON *.* TO $move_user;
    GRANT SELECT ON system.part_moves_between_shards TO $move_user;

    GRANT CLUSTER, KILL TRANSACTION ON *.* TO $partial_txn_user;

    GRANT CLUSTER ON *.* TO $partial_move_user;
    GRANT SELECT ON system.part_moves_between_shards TO $partial_move_user;
"

cluster="test_shard_localhost"
# Match no live transaction and no live part move, so every allowed arm below is a no-op.
tid="(1, 1, '00000000-0000-0000-0000-000000000000')"
task_uuid="'00000000-0000-0000-0000-000000000000'"

# Report the privilege the server asked for and let the reference hold the expected mapping. The
# privilege name is what discriminates: an ACCESS_DENIED-only assertion would also pass when the
# CLUSTER check, which runs before the required-access check, is what fired.
required_privilege() {
    $CLICKHOUSE_CLIENT --distributed_ddl_output_mode none --user "$1" --query "$2" 2>&1 |
        sed -n "/necessary to have the grant/{s/.*grant \(.*\)\. (ACCESS_DENIED).*/\1/p;q;}"
}

# Each statement must name over ON CLUSTER the same privilege its local spelling names.
while IFS= read -r statement; do
    echo "${statement%% ON CLUSTER*} -> $(required_privilege "$cluster_user" "$statement")"
done <<EOF
KILL TRANSACTION ON CLUSTER $cluster WHERE tid = $tid
KILL PART_MOVE_TO_SHARD ON CLUSTER $cluster WHERE task_uuid = $task_uuid
SYSTEM STOP THREAD FUZZER ON CLUSTER $cluster
SYSTEM START THREAD FUZZER ON CLUSTER $cluster
SYSTEM RESET COVERAGE ON CLUSTER $cluster
EOF

# Both KILL statements need two privileges, and only the first one reported above. Each user below
# holds the first and lacks the second, so the second is asserted on its own.
# For KILL TRANSACTION the local spelling reports the same missing privilege. For
# KILL PART_MOVE_TO_SHARD it does not: the local path checks the move privileges per matched row, and
# there is no row here, while the initiator cannot know the target tables and so requires them
# globally. That is stronger in scope than the local check, never weaker.
echo "KILL TRANSACTION without system.transactions -> $(required_privilege "$partial_txn_user" "KILL TRANSACTION ON CLUSTER $cluster WHERE tid = $tid")"
echo "KILL PART_MOVE_TO_SHARD without move privileges -> $(required_privilege "$partial_move_user" "KILL PART_MOVE_TO_SHARD ON CLUSTER $cluster WHERE task_uuid = $task_uuid")"

# In-range control: holding the statement privileges without CLUSTER is refused by the earlier check,
# so a mapping that refuses everything would not produce the five lines above.
echo "no CLUSTER grant -> $(required_privilege "$no_cluster_user" "KILL TRANSACTION ON CLUSTER $cluster WHERE tid = $tid")"

# The two statements whose privileges can be granted in full are allowed in both spellings, which
# proves the new elements are a gate rather than an unconditional refusal. The local half is asserted
# too: a user who is cluster-allowed while locally refused is the bypass this test exists to catch.
# The thread fuzzer and coverage statements get no allowed arm, because executing them would change
# server-global state that concurrent tests read and neither takes an argument that lets the host
# reject them harmlessly.
allowed() {
    local out
    out=$($CLICKHOUSE_CLIENT --distributed_ddl_output_mode none --user "$1" --query "$2" 2>&1)
    if [ -z "$out" ]; then
        echo "$3 -> allowed"
    else
        echo "$3 -> FAIL: $out"
    fi
}

allowed "$kill_txn_user" "KILL TRANSACTION WHERE tid = $tid" "KILL TRANSACTION local"
allowed "$kill_txn_user" "KILL TRANSACTION ON CLUSTER $cluster WHERE tid = $tid" "KILL TRANSACTION on cluster"
allowed "$move_user" "KILL PART_MOVE_TO_SHARD WHERE task_uuid = $task_uuid" "KILL PART_MOVE_TO_SHARD local"
allowed "$move_user" "KILL PART_MOVE_TO_SHARD ON CLUSTER $cluster WHERE task_uuid = $task_uuid" "KILL PART_MOVE_TO_SHARD on cluster"
