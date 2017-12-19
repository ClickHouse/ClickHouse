set timeout -1

log_user 0

spawn clickhouse-client --pager=head

expect ":) "

send "select name from system.settings union all select name from system.settings union all select name from system.settings union all select name from system.settings union all select name from system.settings union all select name from system.settings union all select name from system.settings union all select name from system.settings\r"

expect ":) "

send "quit\r"

expect eof
