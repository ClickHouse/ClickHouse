#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


session_log_query_prefix="
system flush logs;
select distinct type, user, auth_type, toString(client_address)!='::ffff:0.0.0.0' as a, client_port!=0 as b, interface from system.session_log
where user in ('default', 'nonexistsnt_user_1119', '   ', ' INTERSERVER SECRET ')
and interface in ('HTTP', 'TCP', 'TCP_Interserver')
and (user != 'default' or (a=1 and b=1)) -- FIXME: we should not write uninitialized address and port (but we do sometimes)
and event_time >= now() - interval 5 minute"

$CLICKHOUSE_CLIENT -m -q "
select * from remote('127.0.0.2', system, one, 'default', '');
select * from remote('127.0.0.2', system, one, 'default', 'wrong password'); -- { serverError AUTHENTICATION_FAILED }
select * from remote('127.0.0.2', system, one, 'nonexistsnt_user_1119', ''); -- { serverError AUTHENTICATION_FAILED }
set receive_timeout=1;
select * from remote('127.0.0.2', system, one, ' INTERSERVER SECRET ', ''); -- { serverError NO_REMOTE_SHARD_AVAILABLE }
set receive_timeout=300;
select * from remote('127.0.0.2', system, one, '   ', ''); -- { serverError AUTHENTICATION_FAILED }

select * from url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');
select * from url('http://127.0.0.1:8123/?query=select+1&user=default&password=wrong', LineAsString, 's String'); -- { serverError RECEIVED_ERROR_FROM_REMOTE_IO_SERVER }
select * from url('http://127.0.0.1:8123/?query=select+1&user=nonexistsnt_user_1119', LineAsString, 's String'); -- { serverError RECEIVED_ERROR_FROM_REMOTE_IO_SERVER }
select * from url('http://127.0.0.1:8123/?query=select+1&user=+INTERSERVER+SECRET+', LineAsString, 's String'); -- { serverError RECEIVED_ERROR_FROM_REMOTE_IO_SERVER }
select * from url('http://127.0.0.1:8123/?query=select+1&user=+++', LineAsString, 's String'); -- { serverError RECEIVED_ERROR_FROM_REMOTE_IO_SERVER }

select * from cluster('test_cluster_interserver_secret', system, one);

$session_log_query_prefix and type != 'Logout' order by type, user, interface;
"

# Wait for logout events.
for _ in {1..10}
do
    if [ "`$CLICKHOUSE_CLIENT -q "$session_log_query_prefix and type = 'Logout'" | wc -l`" -eq 3 ]
    then
        break
    fi
    sleep 2
done

$CLICKHOUSE_CLIENT -q "$session_log_query_prefix and type = 'Logout' order by user, interface"
