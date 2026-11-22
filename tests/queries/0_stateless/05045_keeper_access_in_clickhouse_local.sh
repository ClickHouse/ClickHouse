#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the assertion these paths used to hit is only compiled into debug and sanitizer
# builds, so the check below cannot fail in the fast test's release build.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Reaching [Zoo]Keeper from an application that is not the server used to take the process down on two
# paths: replacing a session that has been closed, and the transaction log's cleanup of old entries.
# Each path gets its own `clickhouse-local`, both so an abort stays contained (a server-side abort is
# reported as SERVER_DIED and could not be inverted) and so a single arm cannot cover for the other.
#
# `implementation = testkeeper` runs the coordination service inside the process, so neither arm needs
# an external [Zoo]Keeper, a port, or a shared namespace.

# `clickhouse-local` exits with a ClickHouse error code, which the shell truncates to its low byte, so
# an ordinary error can land in the same numeric range as a signal status. Name the statuses that mean
# death by signal instead of testing the range.
signal_status()
{
    case "$1" in
        134) echo "died from signal 6" ;;
        139) echo "died from signal 11" ;;
        *) echo "no signal death" ;;
    esac
}

# Both lines each arm prints are mandatory. A clean exit on its own is also what a process that never
# reached [Zoo]Keeper produces, so the trailing marker is what shows the statements actually ran.

session_config="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_session.xml"
session_out="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_session.out"
cat > "$session_config" <<EOF
<clickhouse>
    <keeper_map_path_prefix>/${CLICKHOUSE_TEST_UNIQUE_NAME}</keeper_map_path_prefix>
    <zookeeper>
        <implementation>testkeeper</implementation>
    </zookeeper>
</clickhouse>
EOF

# `SYSTEM RECONNECT ZOOKEEPER` closes the session and keeps the handle, so the next access has to
# replace it: the first table establishes a session, the second one runs into the replacement. The
# number of replaced sessions is part of the marker, so an arm that never replaced one cannot print
# the expected line.
${CLICKHOUSE_LOCAL} --config-file="$session_config" \
    --path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_session_d" --query "
    CREATE TABLE km1 (k String, v UInt64) ENGINE = KeeperMap('/km1') PRIMARY KEY k;
    SYSTEM RECONNECT ZOOKEEPER;
    CREATE TABLE km2 (k String, v UInt64) ENGINE = KeeperMap('/km2') PRIMARY KEY k;
    SELECT 'session replaced ' || toString(value) FROM system.metrics WHERE metric = 'ZooKeeperSessionExpired';
" > "$session_out" 2>/dev/null
signal_status "$?"
cat "$session_out"

txn_config="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_txn.xml"
txn_out="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_txn.out"
cat > "$txn_config" <<EOF
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
    <zookeeper>
        <implementation>testkeeper</implementation>
    </zookeeper>
</clickhouse>
EOF

# Only a transaction that writes reaches the log, and the cleanup runs right after the log picks the
# new entry up, which `SYSTEM SYNC TRANSACTION LOG` waits for. The database is named explicitly because
# a transaction needs a table the default database of `clickhouse-local` does not necessarily provide.
# The insert takes its rows from the query, so standard input is closed rather than left for it to read.
${CLICKHOUSE_LOCAL} --config-file="$txn_config" \
    --path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_txn_d" --query "
    CREATE DATABASE txn ENGINE = Atomic;
    CREATE TABLE txn.t (a UInt64) ENGINE = MergeTree ORDER BY a;
    BEGIN TRANSACTION;
    INSERT INTO txn.t VALUES (1);
    COMMIT;
    SYSTEM SYNC TRANSACTION LOG;
    SELECT 'transaction committed';
" > "$txn_out" 2>/dev/null < /dev/null
signal_status "$?"
cat "$txn_out"

rm -f "$session_config" "$session_out" "$txn_config" "$txn_out"
rm -rf "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_session_d" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_txn_d"
