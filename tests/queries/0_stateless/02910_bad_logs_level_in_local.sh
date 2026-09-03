#!/usr/bin/expect -f

set script_path [info script]
set CURDIR [file dirname [file normalize $script_path]]

set CLICKHOUSE_LOCAL ""
set CLICKHOUSE_LOCAL [exec bash -c "source $CURDIR/../shell_config.sh && echo \$CLICKHOUSE_LOCAL"]

log_user 0
set timeout 30
match_max 100000

spawn bash -c "$CLICKHOUSE_LOCAL"

expect ":) "
send -- "SET send_logs_level = 't'\r"
expect "Unexpected value of LogsLevel:" {} timeout {exit 1}
expect ":) "
send -- "exit\r"
expect eof

