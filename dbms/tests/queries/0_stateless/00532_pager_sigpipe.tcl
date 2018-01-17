set timeout -1

log_user 0

spawn sh -c $::env(CLICKHOUSE_CLIENT) --pager=head

expect ":) "

send "select name from system.settings union all select name from system.settings union all select name from system.settings union all select name from system.settings union all select name from system.settings union all select name from system.settings union all select name from system.settings union all select name from system.settings\r"

expect ":) "

send "quit\r"

expect eof
