#!/usr/bin/expect -f

set basedir [file dirname $argv0]
set basename [file tail $argv0]
exp_internal -f $env(CLICKHOUSE_TMP)/$basename.debuglog 0
set history_file $env(CLICKHOUSE_TMP)/$basename.history

log_user 1
set timeout 60
match_max 100000

expect_after {
    # Do not ignore eof from expect
    -i $any_spawn_id eof { exp_continue }
    # A default timeout action is to do nothing, change it to fail
    -i $any_spawn_id timeout { exit 1 }
}

spawn bash -c "source $basedir/../shell_config.sh ; \$CLICKHOUSE_CLIENT --query 'SELECT 1'"
expect "│ 1 │"
expect "└───┘"
expect eof

spawn bash -c "source $basedir/../shell_config.sh ; \$CLICKHOUSE_LOCAL --query 'SELECT 2'"
expect "│ 2 │"
expect "└───┘"
expect eof
