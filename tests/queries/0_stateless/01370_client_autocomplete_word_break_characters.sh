#!/usr/bin/expect -f

log_user 0
set timeout 1
match_max 100000

spawn clickhouse-client
expect ":) "

# Make a query
send -- "SET max_distributed"
expect "SET max_distributed"

# Wait for suggestions to load, they are loaded in background
set is_done 0
while {$is_done == 0} {
    send -- "\t"
    expect {
        "_connections" {
            set is_done 1
        }
        default {
            sleep 1
        }
    }
}

send -- "\3\4"
expect eof
