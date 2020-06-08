#!/usr/bin/expect -f

log_user 0
set timeout 1
match_max 100000

spawn clickhouse-client
expect ":) "

expect {
    "Cannot load data" {
        send_user "Error\n"
    }
}

send -- "\4"
expect eof
