#!/usr/bin/expect -f

set script_path [info script]
set CURDIR [file dirname [file normalize $script_path]]

set CLICKHOUSE_CLIENT_BINARY ""
set CLICKHOUSE_CLIENT_BINARY [exec bash -c "source $CURDIR/../shell_config.sh && echo \$CLICKHOUSE_CLIENT_BINARY"]

log_user 0

set timeout 60

match_max 100000

if ![info exists env(CLICKHOUSE_PORT_TCP)] {set env(CLICKHOUSE_PORT_TCP) 9000}

spawn "$CLICKHOUSE_CLIENT_BINARY" --multiline --disable_suggestion --port "$env(CLICKHOUSE_PORT_TCP)"
expect ":) "

# Make a query
send -- "SELECT 1\r"
send -- "-- xxx\r"
send -- ", 2\r"
send -- ";"

# For some reason this sleep is required for this test to work properly
sleep 1
send -- "\r"

expect {
    "│ 1 │ 2 │" { }
    timeout { exit 1 }
}

expect ":) "

send -- ""
expect {
    eof { exit 0 }
    timeout { exit 1 }
}
