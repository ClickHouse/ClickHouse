#!/usr/bin/expect -f
set basedir [file dirname $argv0]
set basename [file tail $argv0]
if {[info exists env(CLICKHOUSE_TMP)]} {
    set CLICKHOUSE_TMP $env(CLICKHOUSE_TMP)
} else {
    set CLICKHOUSE_TMP "."
}
exp_internal -f $CLICKHOUSE_TMP/$basename.debuglog 0

set timeout 30
log_user 0
match_max 100000
set stty_init "rows 25 cols 120"

expect_after {
    -i $any_spawn_id eof { exp_continue }
    -i $any_spawn_id timeout { exit 1 }
}


spawn bash
send "source $basedir/../shell_config.sh\r"

send "\$CLICKHOUSE_CLIENT --query 'CREATE DATABASE IF NOT EXISTS test;' >/dev/null 2>&1\r"
expect "\r"

send "\$CLICKHOUSE_CLIENT --query 'CREATE TABLE IF NOT EXISTS test.simple_table (id UInt64, name String) ENGINE = Memory;' >/dev/null 2>&1\r"
expect "\r"

send "\$CLICKHOUSE_CLIENT --processed-rows --query \"INSERT INTO test.simple_table SELECT 1 AS id, 'Alice' AS name;\" \r"
expect "Processed rows: 1"

send "\$CLICKHOUSE_CLIENT --processed-rows --query \"INSERT INTO test.simple_table SELECT number + 1 AS id, \['Alice','Bob','Charlie'\]\[number + 1\] AS name FROM numbers(3);\" \r"
expect "Processed rows: 3"

send "echo OK\r"
expect "OK"

send "exit\r"
expect eof
