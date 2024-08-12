#!/usr/bin/expect -f

log_user 0
set timeout 60
match_max 100000

spawn bash -c "clickhouse-local"

expect ":) "
send -- "SET send_logs_level = 't'\r"
expect "Exception on client:"
expect ":) "
send -- "exit\r"
expect eof

