#!/usr/bin/expect -f
# Tags: no-fasttest

log_user 0
set timeout 60
match_max 100000

if {![info exists env(CLICKHOUSE_PORT_TCP)]} { set env(CLICKHOUSE_PORT_TCP) 9000 }

spawn clickhouse-client --disable_suggestion
expect ":) ";

send -- "SELECT x'A0' FORMAT TSV;\r"

# Validate that this doesn't cause the client to crash.
#
expect {
    -re {.+\r\n} { }         ;# expect atleast one byte and a newline to pass
    timeout      { exit 1 }  ;# if timeouts, then fail
    eof          { exit 1 }  ;# if client crashes, then fail
}

expect ":) "

send -- "\004"
expect eof
