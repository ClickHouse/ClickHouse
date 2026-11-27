#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When a separate connection loading the command line suggestions fails with
# `USER_SESSION_LIMIT_EXCEEDED`, the client loads them over the main connection - on the same
# session as the regular queries. That query is sent with the terminating empty block, so when
# it fails with a server exception, the protocol stays in sync and the server preserves the
# connection - the next query of the session must not resynchronize the connection with a round
# trip: a `Pong` that does not arrive within `sync_request_timeout` is indistinguishable from a
# closed connection, and the client would silently reconnect - losing its temporary tables,
# current database and session settings.
#
# The check below is deterministic: all the traffic goes through a proxy that delays everything
# the server sends, while the client is configured with `sync_request_timeout` smaller than the
# delay. The user is limited to one session, so the suggestions loader fails with
# `USER_SESSION_LIMIT_EXCEEDED` - `--wait_for_suggestions_to_load` makes the client wait for
# that before the first prompt. The temporary table is created by the initial queries of the
# same session (`--interactive` with `--query` runs them on the same connection before the
# interactive loop). The first interactive query then triggers the fallback, which fails on the
# server: the suggestions query returns many rows, which the profile of the user rejects with
# `max_result_rows = 1`. A client that pings after that failure cannot get the `Pong` in time,
# so it reconnects and loses the temporary table. A client that continues without the ping is
# unaffected - the delay only makes the queries slower, which no timeout here objects to.

USER_NAME="user_04814_${CLICKHOUSE_DATABASE}"
PROFILE_NAME="profile_04814_${CLICKHOUSE_DATABASE}"
PROXY_PORT_FILE="${CLICKHOUSE_TMP}/04814_proxy_${CLICKHOUSE_DATABASE}.port"
CLIENT_CONFIG="${CLICKHOUSE_TMP}/04814_client_${CLICKHOUSE_DATABASE}.xml"

rm -f "$PROXY_PORT_FILE"

python3 "$CUR_DIR"/helpers/delaying_tcp_proxy.py "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" 2 "$PROXY_PORT_FILE" &
PROXY_PID=$!
# shellcheck disable=SC2064
trap "kill $PROXY_PID 2>/dev/null; rm -f '$PROXY_PORT_FILE' '$CLIENT_CONFIG'" EXIT

# max_sessions_for_user is not allowed to be set by a user directly, only via a profile.
${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER_NAME};
    DROP SETTINGS PROFILE IF EXISTS ${PROFILE_NAME};
    CREATE SETTINGS PROFILE ${PROFILE_NAME}
        SETTINGS max_result_rows = 1, result_overflow_mode = 'throw', max_sessions_for_user = 1;
    CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'password_04814'
        SETTINGS PROFILE '${PROFILE_NAME}';
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

# The address of the proxy replaces the address of the server, and the suggestions - disabled
# for the expect tests by default - are enabled back, since they are what this test is about.
# The rest of the options is kept.
CLIENT_OPT=$(echo "${CLICKHOUSE_CLIENT_EXPECT_OPT}" | sed "s/--host=[^ ]*//g; s/--port=[^ ]*//g; s/--disable_suggestion//g")

expect << EOF
log_user 0
set timeout 60
match_max 100000
exp_internal -f $CLICKHOUSE_TMP/$(basename "${BASH_SOURCE[0]}").debuglog 0
expect_after {
    -i \$any_spawn_id eof { exp_continue }
    -i \$any_spawn_id timeout { puts "TIMEOUT"; exit 1 }
}

spawn bash -c "$CLICKHOUSE_CLIENT_BINARY $CLIENT_OPT --config-file $CLIENT_CONFIG --host 127.0.0.1 --port $PROXY_PORT --user $USER_NAME --password password_04814 --interactive --wait_for_suggestions_to_load --query 'CREATE TEMPORARY TABLE t_04814 (x UInt8); INSERT INTO t_04814 VALUES (1); SELECT x + 100 FROM t_04814'"

# The session state must really exist before the suggestions fallback runs, otherwise the check
# at the end would report a lost session for a reason that has nothing to do with the connection.
expect {
    "101" { }
    "UNKNOWN_TABLE" { puts "the temporary table has not been created"; exit 1 }
    "Not enough privileges" { puts "the temporary table has not been created"; exit 1 }
}
expect ":) "

# Reading this input is what triggers the suggestions fallback on the main connection, before
# the query itself is processed. The exception message is the check that the fallback really ran
# and failed on the server.
send -- "SELECT x + 41 FROM t_04814\r"
expect "Suggestions loading exception"
puts "the suggestions fallback failed on the server"

# The result (42) does not appear in the echo of the line that is sent, so matching it means the
# query has really been executed. A client that reconnected after the failed suggestions
# exchange prints "Connected to" and loses the temporary table.
expect {
    "42" { puts "the session state is kept after a suggestions failure" }
    "UNKNOWN_TABLE" { puts "UNKNOWN_TABLE: the client lost the session"; exit 1 }
    "Connected to" { puts "the client reconnected after the suggestions failure"; exit 1 }
    "SESSION_LIMIT" { puts "the client reconnected after the suggestions failure"; exit 1 }
}
expect ":) "

send -- "exit\r"
expect eof
EOF
EXPECT_STATUS=$?

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER_NAME};
    DROP SETTINGS PROFILE IF EXISTS ${PROFILE_NAME};
"

# The teardown must not mask a failure of the expect script above.
exit $EXPECT_STATUS
