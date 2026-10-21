#!/usr/bin/expect -f
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest

log_user 0
set timeout 60
match_max 100000

spawn clickhouse-local --progress --query "SELECT sum(number % 100000000 = 12345678 ? sleep(0.1) : 1) FROM numbers(1000000000)"

expect {
    "▏" { exit 0 }
    "▎" { exit 0 }
    "▍" { exit 0 }
    "▌" { exit 0 }
    "▋" { exit 0 }
    "▋" { exit 0 }
    "▊" { exit 0 }
    "▉" { exit 0 }
    timeout { exit 1 }
}
