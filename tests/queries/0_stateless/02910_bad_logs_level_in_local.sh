#!/usr/bin/expect -f

log_user 0
set timeout 30
match_max 100000

spawn bash -c "clickhouse-local"

expect ":) "
send -- "SET send_logs_level = 't'\r"
expect "Unexpected value of LogsLevel:" {} timeout {exit 1}
expect ":) "
send -- "exit\r"
expect eof

