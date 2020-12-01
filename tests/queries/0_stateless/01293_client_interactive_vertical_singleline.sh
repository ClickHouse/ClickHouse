#!/usr/bin/expect -f

log_user 0
set timeout 60
spawn clickhouse-client
match_max 100000

expect ":) "

send -- "SELECT 1\r"
expect "│ 1 │"
expect ":) "

send -- "SELECT 1\\G\r"
expect "Row 1:"
expect "1: 1"
expect ":) "

send -- "SELECT 1\\\r"
expect ":-] "
send -- ", 2\r"
expect "│ 1 │ 2 │"
expect ":) "

send -- "SELECT 1\\\r"
expect ":-] "
send -- ", 2\\G\r"
expect "Row 1:"
expect "1: 1"
expect "2: 2"
expect ":) "

send -- ""
expect eof
