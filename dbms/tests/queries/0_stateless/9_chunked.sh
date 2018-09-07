#!/usr/bin/env bash
set -e
#set -x

# sudo tcpdump -X -i lo port 8123

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo -e '1\tGroup' > groups && ${CLICKHOUSE_CURL} -F 'groups=@groups' "${CLICKHOUSE_URL}?query=select+UserName%2C+GroupName+from+%28select+%27User%27+as+UserName%2C+1+as+GroupId%29+any+left+join+groups+using+GroupId&groups_structure=GroupId+UInt8,+GroupName+String" --header "Transfer-Encoding: chunked"
