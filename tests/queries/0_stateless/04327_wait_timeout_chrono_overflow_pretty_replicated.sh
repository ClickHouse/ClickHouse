#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# no-fasttest: creates a Replicated database (needs ZooKeeper).

# Two more user-controlled wait timeouts must saturate instead of overflowing the chrono ms/sec->ns
# conversion inside condition_variable::wait_for (UBSan signed-integer-overflow, immediate timeout in
# release). output_format_pretty_squash_consecutive_ms drives PrettyBlockOutputFormat's writing thread;
# database_replicated_initial_query_timeout_sec drives the Replicated-database initial-query wait.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A huge output_format_pretty_squash_consecutive_ms must not overflow the Pretty writer-thread wait.
$CLICKHOUSE_CLIENT --output_format_pretty_squash_consecutive_ms=100000000000000000 \
    -q "SELECT number FROM numbers(3) FORMAT Pretty" > /dev/null && echo "pretty_ok"

# A huge database_replicated_initial_query_timeout_sec must not overflow the initial-query wait.
RDB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${RDB}"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE DATABASE ${RDB} ENGINE = Replicated('/test/${RDB}/{shard}', '{shard}', '{replica}')"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --database_replicated_initial_query_timeout_sec=100000000000 \
    -q "CREATE TABLE ${RDB}.t (n UInt64) ENGINE = MergeTree ORDER BY n" && echo "replicated_ddl_ok"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${RDB}"
