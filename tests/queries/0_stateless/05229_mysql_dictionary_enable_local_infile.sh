#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the MySQL integration is not available in the fast test build.

# `enable_local_infile` sets `MYSQL_OPT_LOCAL_INFILE` on the connection, which lets the MySQL endpoint
# ask the client for the contents of a file of its choosing: the server opens it with its own
# privileges. A dictionary created with a DDL query may not enable it, whether at the source itself or
# at one of its replicas. A dictionary defined in a server configuration file is written by an operator
# and keeps working; that route is covered by tests/integration/test_dictionaries_mysql.
#
# No MySQL server is needed: the rejection happens while the source is instantiated, before any
# connection. A source is instantiated when the dictionary is loaded, so the reload is what surfaces
# the rejection if the `CREATE` itself did not.
#
# The arms that must NOT be rejected point at `127.0.0.1` port 1, where nothing listens, so they end in
# a connection failure. That failure is asserted too: it is what proves the arm reached an actual
# connection attempt instead of being rejected for some unrelated reason, which is what keeps the
# "no rejection" oracle from passing vacuously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

REJECTED="cannot be enabled in a dictionary created with a DDL query"
CONNECTION_FAILED="ALL_CONNECTION_TRIES_FAILED"

create_and_load() {
    local name="$1"
    local source="$2"
    $CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS ${name}"
    $CLICKHOUSE_CLIENT --query "
        CREATE DICTIONARY ${name} (id UInt32, name String) PRIMARY KEY id
        SOURCE(MYSQL(${source}))
        LAYOUT(FLAT()) LIFETIME(0)"
    $CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY ${name}"
    $CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS ${name}"
}

expect_rejected() {
    echo "--- $1"
    if create_and_load "$2" "$3" 2>&1 | grep -q "$REJECTED"; then
        echo "OK"
    else
        echo "FAIL: expected an error matching: $REJECTED"
    fi
}

expect_not_rejected() {
    echo "--- $1"
    local output
    output=$(create_and_load "$2" "$3" 2>&1)
    if echo "$output" | grep -q "$REJECTED"; then
        echo "FAIL: unexpected error matching: $REJECTED"
    elif ! echo "$output" | grep -q "$CONNECTION_FAILED"; then
        echo "FAIL: expected the source to reach a connection attempt"
    else
        echo "OK"
    fi
}

expect_rejected "enabled at the source" dict_infile_source \
    "HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' ENABLE_LOCAL_INFILE 1"

expect_rejected "enabled at a replica" dict_infile_replica \
    "USER 'u' PASSWORD 'p' DB 'd' TABLE 't'
     REPLICA(PRIORITY 1 HOST '127.0.0.1' PORT 1 ENABLE_LOCAL_INFILE 1)"

# Every <replica> is checked, not just the first: the pool resolves the option per replica and
# fails over between them, so a later replica enabling it is enough to reach MYSQL_OPT_LOCAL_INFILE.
expect_rejected "enabled at a later replica, disabled at the first" dict_infile_replica_second \
    "USER 'u' PASSWORD 'p' DB 'd' TABLE 't' PORT 1
     REPLICA(PRIORITY 1 HOST '127.0.0.1' ENABLE_LOCAL_INFILE 0)
     REPLICA(PRIORITY 2 HOST '127.0.0.1' ENABLE_LOCAL_INFILE 1)"

# A replica that does not set the option inherits it from the source, so the source is where this one
# is enabled. The guard resolves the value the same way the connection pool does.
expect_rejected "inherited by a replica from the source" dict_infile_inherited \
    "USER 'u' PASSWORD 'p' DB 'd' TABLE 't' PORT 1 ENABLE_LOCAL_INFILE 1
     REPLICA(PRIORITY 1 HOST '127.0.0.1')"

# Explicitly disabled is what the connection does by default, so it is accepted.
expect_not_rejected "disabled at the source" dict_infile_disabled \
    "HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' ENABLE_LOCAL_INFILE 0"

# Only per-replica pools are built when the source has replicas, so a replica that turns the option off
# never enables it, whatever the source says. Two replicas, because the option is resolved per replica.
expect_not_rejected "disabled at every replica, enabled at the source" dict_infile_replicas_disabled \
    "USER 'u' PASSWORD 'p' DB 'd' TABLE 't' PORT 1 ENABLE_LOCAL_INFILE 1
     REPLICA(PRIORITY 1 HOST '127.0.0.1' ENABLE_LOCAL_INFILE 0)
     REPLICA(PRIORITY 2 HOST '127.0.0.1' ENABLE_LOCAL_INFILE 0)"
