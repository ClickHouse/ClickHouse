#!/usr/bin/expect -f

set basedir [file dirname $argv0]
set basename [file tail $argv0]
if {[info exists env(CLICKHOUSE_TMP)]} {
    set CLICKHOUSE_TMP $env(CLICKHOUSE_TMP)
} else {
    set CLICKHOUSE_TMP "."
}
exp_internal -f $CLICKHOUSE_TMP/$basename.debuglog 0
set history_file $CLICKHOUSE_TMP/$basename.history

log_user 0
set timeout 60
match_max 100000

expect_after {
    # Do not ignore eof from expect
    -i $any_spawn_id eof { exp_continue }
    # A default timeout action is to do nothing, change it to fail
    -i $any_spawn_id timeout { exit 1 }
}

# A single-line comment inside a multiline query must not hide the rest of the query:
# input keeps being collected after it, and the final `;` runs `SELECT 1, 2`.
spawn bash -c "source $basedir/../shell_config.sh ; \$CLICKHOUSE_CLIENT_BINARY \$CLICKHOUSE_CLIENT_EXPECT_OPT --multiline --history_file=$history_file"
expect ":) "

send -- "SELECT 1\r"
expect ":-] "
send -- "-- xxx\r"
expect ":-] "
send -- ", 2\r"
expect ":-] "
send -- ";\r"
expect "│ 1 │ 2 │"
expect ":) "

send -- "\4"
expect eof
