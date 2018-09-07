#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

#$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.groups"
#$CLICKHOUSE_CLIENT --query="CREATE TABLE test.groups (x UInt64, s String) ENGINE = Memory"


echo -e '1\tGroup' > groups && ${CLICKHOUSE_CURL} -vvv -F 'groups=@groups' "${CLICKHOUSE_URL}?query=select+UserName%2C+GroupName+from+%28select+%27User%27+as+UserName%2C+1+as+GroupId%29+any+left+join+groups+using+GroupId&groups_structure=GroupId+UInt8,+GroupName+String" --header "Transfer-Encoding: chunked"

#echo -e '1\tGroup' > groups && ${CLICKHOUSE_CURL}  -F 'groups=@groups' "${CLICKHOUSE_URL}?query=select+numberfrom+%28select+%27User%27+as+UserName%2C+1+as+GroupId%29+any+left+join+system.numbers+using+GroupId&groups_structure=GroupId+UInt8,+GroupName+String" --header "Transfer-Encoding: chunked"

#$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.groups"
