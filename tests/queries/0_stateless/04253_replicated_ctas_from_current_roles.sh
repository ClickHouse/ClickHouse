#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-shared-merge-tree
#
# Regression test for the `BuzzHouse (amd_tsan)` finding `STID: 2436-3d64`:
#   `Logical error: 'No user in current context, it's a bug'`
#
# Reading from `system.current_roles` or `system.enabled_roles` inside a
# `CREATE TABLE ... AS SELECT` executed by the replicated DDL worker triggers
# the error because the DDL worker's query context has no user attached when
# `distributed_ddl_use_initial_user_and_roles` is disabled (the default).
# `StorageSystemCurrentRoles::fillData` and `StorageSystemEnabledRoles::fillData`
# unconditionally called `Context::getUser`, which throws `LOGICAL_ERROR` and
# aborts the server in debug/sanitizer builds.
#
# The exception kills the whole `clickhouse-server` process, so we cannot
# exercise it against the shared test runner server: a server abort would be
# classified as `SERVER_DIED` and the `Bugfix validation` job would report
# `Failed to reproduce the bug` instead of an inverted `FAIL`. We spawn an
# isolated `clickhouse-server` subprocess (with `testkeeper` and its own
# ports) and exercise the bug-triggering scenario there; the subprocess
# aborts on master HEAD and stays alive once the fix is applied. The
# bugfix-validation framework sees the resulting `BUG`/`OK` output diff.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR/data" "$WORK_DIR/user_files"

# Pick ports well above the test server's range. `Tags: no-parallel` guarantees
# no other stateless test runs concurrently in the same container, and each CI
# runner has its own network namespace, so a static offset is safe.
PORT_TCP=$(( CLICKHOUSE_PORT_TCP + 8001 ))
PORT_HTTP=$(( CLICKHOUSE_PORT_TCP + 8002 ))
PORT_INTERSERVER=$(( CLICKHOUSE_PORT_TCP + 8003 ))

cat > "$WORK_DIR/config.xml" <<EOF
<?xml version="1.0"?>
<clickhouse>
    <logger>
        <level>information</level>
        <log>${WORK_DIR}/server.log</log>
        <errorlog>${WORK_DIR}/server.err.log</errorlog>
        <stderr>${WORK_DIR}/stderr.log</stderr>
        <console>0</console>
    </logger>
    <tcp_port>${PORT_TCP}</tcp_port>
    <http_port>${PORT_HTTP}</http_port>
    <interserver_http_port>${PORT_INTERSERVER}</interserver_http_port>
    <interserver_http_host>127.0.0.1</interserver_http_host>
    <listen_host>127.0.0.1</listen_host>
    <path>${WORK_DIR}/data/</path>
    <user_files_path>${WORK_DIR}/user_files/</user_files_path>
    <max_server_memory_usage>10737418240</max_server_memory_usage>
    <max_concurrent_queries>50</max_concurrent_queries>
    <mark_cache_size>1073741824</mark_cache_size>
    <send_crash_reports>
        <enabled>false</enabled>
    </send_crash_reports>
    <macros>
        <shard>shard1</shard>
        <replica>replica1</replica>
    </macros>
    <zookeeper>
        <implementation>testkeeper</implementation>
    </zookeeper>
    <users_config>users.xml</users_config>
    <default_profile>default</default_profile>
    <default_database>default</default_database>
</clickhouse>
EOF

cat > "$WORK_DIR/users.xml" <<EOF
<?xml version="1.0"?>
<clickhouse>
    <profiles><default/></profiles>
    <users>
        <default>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <quotas><default/></quotas>
</clickhouse>
EOF

$CLICKHOUSE_BINARY server --config-file="$WORK_DIR/config.xml" --pid-file="$WORK_DIR/pid" \
    > "$WORK_DIR/stdout.log" 2>&1 &
SRV_PID=$!

# Make sure we clean up the subprocess server even on early exit.
trap 'kill -9 $SRV_PID 2>/dev/null; rm -rf "$WORK_DIR"' EXIT

# Wait for the subprocess server to accept connections (up to ~60s).
for _ in $(seq 1 60); do
    if $CLICKHOUSE_BINARY client --host 127.0.0.1 --port "$PORT_TCP" \
            --send_logs_level=none --query "SELECT 1" > /dev/null 2>&1; then
        break
    fi
    sleep 1
    if ! kill -0 "$SRV_PID" 2>/dev/null; then
        echo "subprocess clickhouse-server failed to start"
        tail -30 "$WORK_DIR/server.log" 2>/dev/null || true
        exit 1
    fi
done

# Trigger the bug: a CTAS reading `system.current_roles` via the replicated
# DDL worker. With `distributed_ddl_use_initial_user_and_roles=0` (default)
# the worker's query context has no user, so unfixed `fillData` throws
# `LOGICAL_ERROR` and aborts the subprocess server. Both client invocations
# silence their stderr so the parent test stderr never gets `<Fatal>` from
# the subprocess (which would otherwise be picked up by `clickhouse-test` as
# the test server having died).
$CLICKHOUSE_BINARY client --host 127.0.0.1 --port "$PORT_TCP" \
    --send_logs_level=none --query "
        CREATE DATABASE bug
        ENGINE = Replicated('/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/bug', '{shard}', '{replica}');
    " > /dev/null 2>&1

# Default `distributed_ddl_output_mode` makes the client wait for the
# replicated DDL to finish, so an abort in the DDL worker manifests as a
# connection drop on this query rather than asynchronously a few hundred
# milliseconds later. That removes timing dependence on the post-CTAS probe.
$CLICKHOUSE_BINARY client --host 127.0.0.1 --port "$PORT_TCP" \
    --send_logs_level=none --query "
        CREATE TABLE bug.t (role_name String, with_admin_option UInt8, is_default UInt8)
        ENGINE = MergeTree() ORDER BY role_name
        AS SELECT * FROM system.current_roles;
    " > /dev/null 2>&1

# Probe: if the subprocess server still responds, the fix is in place.
# A short wait gives any straggler DDL coordination on testkeeper time to
# settle before we make the final liveness call.
sleep 2
if $CLICKHOUSE_BINARY client --host 127.0.0.1 --port "$PORT_TCP" \
        --send_logs_level=none --connect_timeout=5 \
        --query "SELECT 1" > /dev/null 2>&1; then
    echo "OK"
else
    echo "BUG"
fi
