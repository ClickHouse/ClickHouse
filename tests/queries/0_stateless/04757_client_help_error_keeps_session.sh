#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The interactive `help` command runs its queries over `system.documentation` on the same
# connection as the regular queries of the session, and it sends the terminating empty block
# together with the query. When such an exchange fails with a server exception, the protocol
# stays in sync and the server preserves the connection, so the next query of the session must
# not resynchronize the connection with a round trip: a `Pong` that does not arrive within
# `sync_request_timeout` is indistinguishable from a closed connection, and the client would
# silently reconnect - losing its temporary tables, current database and session settings.
#
# The check below is deterministic: all the traffic goes through a proxy that delays everything
# the server sends, while the client is configured with `sync_request_timeout` smaller than the
# delay. The `help` command fails on the server: the word is a typo, so the exact-match query
# returns nothing and the suggestion query returns many rows, which the profile of the user
# rejects with `max_result_rows = 1`. A client that pings after that failure cannot get the
# `Pong` in time, so it reconnects and loses the temporary table. A client that continues without
# the ping is unaffected - the delay only makes the queries slower, which no timeout here objects
# to.

USER_NAME="user_04757_${CLICKHOUSE_DATABASE}"
PROXY_PORT_FILE="${CLICKHOUSE_TMP}/04757_proxy_${CLICKHOUSE_DATABASE}.port"
CLIENT_CONFIG="${CLICKHOUSE_TMP}/04757_client_${CLICKHOUSE_DATABASE}.xml"

rm -f "$PROXY_PORT_FILE"

python3 "$CUR_DIR"/helpers/delaying_tcp_proxy.py "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" 2 "$PROXY_PORT_FILE" &
PROXY_PID=$!
# shellcheck disable=SC2064
trap "kill $PROXY_PID 2>/dev/null; rm -f '$PROXY_PORT_FILE' '$CLIENT_CONFIG'" EXIT

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER_NAME};
    CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'password_04757'
        SETTINGS max_result_rows = 1, result_overflow_mode = 'throw';
    GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};
    GRANT CREATE TEMPORARY TABLE, SELECT ON *.* TO ${USER_NAME};
    -- The servers in CI require a grant for the table engine of a temporary table.
    GRANT TABLE ENGINE ON Memory TO ${USER_NAME};
"

for _ in {1..300}; do
    [ -s "$PROXY_PORT_FILE" ] && break
    sleep 0.1
done

PROXY_PORT=$(cat "$PROXY_PORT_FILE")

echo '<clickhouse><sync_request_timeout>1</sync_request_timeout></clickhouse>' > "$CLIENT_CONFIG"

# The address of the proxy replaces the address of the server, the rest of the options is kept.
CLIENT_OPT=$(echo "${CLICKHOUSE_CLIENT_EXPECT_OPT}" | sed "s/--host=[^ ]*//g; s/--port=[^ ]*//g")

expect << EOF
log_user 0
set timeout 60
match_max 100000
exp_internal -f $CLICKHOUSE_TMP/$(basename "${BASH_SOURCE[0]}").debuglog 0
expect_after {
    -i \$any_spawn_id eof { exp_continue }
    -i \$any_spawn_id timeout { puts "TIMEOUT"; exit 1 }
}

spawn bash -c "$CLICKHOUSE_CLIENT_BINARY $CLIENT_OPT --config-file $CLIENT_CONFIG --host 127.0.0.1 --port $PROXY_PORT --user $USER_NAME --password password_04757"
expect ":) "

send -- "CREATE TEMPORARY TABLE t_04757 (x UInt8)\r"
expect ":) "

send -- "INSERT INTO t_04757 VALUES (1)\r"
expect ":) "

# The session state must really exist before the help command, otherwise the check at the end
# would report a lost session for a reason that has nothing to do with the connection.
send -- "SELECT x + 100 FROM t_04757\r"
expect {
    "101" { }
    "UNKNOWN_TABLE" { puts "the temporary table has not been created"; exit 1 }
    "Not enough privileges" { puts "the temporary table has not been created"; exit 1 }
}
expect ":) "

send -- "help mergetre\r"
expect "The help command failed"
puts "help failed on the server"
expect ":) "

# The result (42) does not appear in the echo of the line that is sent, so matching it means the
# query has really been executed. A client that reconnected after the failed help command prints
# "Connected to" and loses the temporary table.
send -- "SELECT x + 41 FROM t_04757\r"
expect {
    "42" { puts "the session state is kept after a help failure" }
    "UNKNOWN_TABLE" { puts "UNKNOWN_TABLE: the client lost the session"; exit 1 }
    "Connected to" { puts "the client reconnected after the help failure"; exit 1 }
}
expect ":) "

send -- "exit\r"
expect eof
EOF
EXPECT_STATUS=$?

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER_NAME}"

# The teardown must not mask a failure of the expect script above.
exit $EXPECT_STATUS
