#!/usr/bin/expect -f

log_user 0
set timeout 5
match_max 100000

spawn $env(CLICKHOUSE_CLIENT) --port "$env(CLICKHOUSE_PORT_TCP)"
expect ":) "

# Make a query
send -- "SELECT 'print query id'\r"
expect {
    "Query id: *" { }
    timeout { exit 1 }
}
expect "print query id"
expect ":) "

send -- "\4"
expect eof
