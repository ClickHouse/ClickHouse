#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT xyzgarbage 2>&1 | grep -q "Code: 2003" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -xyzgarbage 2>&1 | grep -q "Bad arguments" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --xyzgarbage 2>&1 | grep -q "Bad arguments" && echo 'OK' || echo 'FAIL'

cat /etc/passwd | sed 's/:/\t/g' | $CLICKHOUSE_CLIENT --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String' xyzgarbage 2>&1 | grep -q "Code: 2003" && echo 'OK' || echo 'FAIL'

cat /etc/passwd | sed 's/:/\t/g' | $CLICKHOUSE_CLIENT --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external -xyzgarbage --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String' 2>&1 | grep -q "Bad arguments" && echo 'OK' || echo 'FAIL'

cat /etc/passwd | sed 's/:/\t/g' | $CLICKHOUSE_CLIENT --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --xyzgarbage --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String' 2>&1 | grep -q "Bad arguments" && echo 'OK' || echo 'FAIL'
