#!/usr/bin/env bash

#\
export CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
#\
. "$CURDIR"/../shell_config.sh
#\
export EDITOR=$CURDIR/01610_client_spawn_editor_open.editor
#\
exec expect -f $CURDIR/01610_client_spawn_editor.sh "$@"

log_user 0
set timeout 1

spawn clickhouse-client
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
