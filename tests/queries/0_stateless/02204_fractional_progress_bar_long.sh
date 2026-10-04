#!/usr/bin/expect -f
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest

log_user 0
set timeout 60
match_max 100000

# The progress bar is drawn only in the space the terminal has left after the progress message,
# and the pseudo-terminal of `spawn` has zero size when `expect` does not run in a terminal itself.
set stty_init "rows 25 cols 120"

spawn clickhouse-local --tmp --progress --query "SELECT sum(number % 100000000 = 12345678 ? sleep(0.1) : 1) FROM numbers(1000000000)"

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
    eof { exit 1 }
}
