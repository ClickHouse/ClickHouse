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

spawn bash -c "source $basedir/../shell_config.sh ; \$CLICKHOUSE_CLIENT --query 'SELECT 1, 2'"
expect "1\t2"
expect eof
