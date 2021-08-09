#!/usr/bin/expect -f

log_user 0
set timeout 60
match_max 100000

if ![info exists env(CLICKHOUSE_PORT_TCP)] {set env(CLICKHOUSE_PORT_TCP) 9000}

set env(EDITOR) [file dirname [file normalize [info script]]]"/01610_client_spawn_editor_open.editor"

spawn clickhouse-client --disable_suggestion
expect ":) "

# Open EDITOR
send -- "\033E"
# Send return
send -- "\r"
expect {
    "│ 1 │" { }
    timeout { exit 1 }
}
expect ":) "

send -- ""
expect eof
