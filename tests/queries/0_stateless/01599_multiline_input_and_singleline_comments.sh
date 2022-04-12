#!/usr/bin/expect -f
# Tags: no-fasttest
# Tag no-fasttest: 180 seconds running

log_user 0
set timeout 60
match_max 100000

if ![info exists env(CLICKHOUSE_PORT_TCP)] {set env(CLICKHOUSE_PORT_TCP) 9000}

spawn clickhouse-client --multiline --disable_suggestion --port "$env(CLICKHOUSE_PORT_TCP)"
expect ":) "

# Make a query
send -- "SELECT 1\r"
expect ":-] "
send -- "-- xxx\r"
expect ":-] "
send -- ", 2\r"
expect ":-] "
send -- ";\r"

expect "│ 1 │ 2 │"
expect ":) "

send -- "\4"
expect eof
