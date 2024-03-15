#!/usr/bin/expect -f

log_user 0

# In some places `-timeout 1` is used to avoid expect to always wait for the whole timeout
set timeout 60

match_max 100000

if ![info exists env(CLICKHOUSE_PORT_TCP)] {set env(CLICKHOUSE_PORT_TCP) 9000}

spawn clickhouse-client --multiline --disable_suggestion --port "$env(CLICKHOUSE_PORT_TCP)"
expect ":) "

# Make a query
send -- "SELECT 1\r"
expect -timeout 1 ":-] "
send -- "-- xxx\r"
expect -timeout 1 ":-] "
send -- ", 2\r"
expect -timeout 1 ":-] "
send -- ";\r"

expect "│ 1 │ 2 │"
expect ":) "

send -- "\4"
expect eof
