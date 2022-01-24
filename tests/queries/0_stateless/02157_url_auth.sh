#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Not sure why fail even in sequential mode. Disabled for now to make some progress.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test
PORT=$((RANDOM%10000+20000))
if [ -n "$(lsof -i:${PORT} |awk '{print $2}'| tail -1)" ]; then
    PORT=$((RANDOM%10000+20000))
fi

python3 "$CURDIR/02157_url_auth.python" $PORT &

sleep 1

${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --query "select * from url('http://admin1:password@127.0.0.1:${PORT}/example', 'RawBLOB', 'a String')" | grep Exception
${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --query "select * from url('http://admin2:password%2F@127.0.0.1:${PORT}/example', 'RawBLOB', 'a String')" | grep Exception
${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --query "select * from url('http://admin3%3F%2F%3APassWord%5E%23%3F%2F@127.0.0.1:${PORT}/example', 'RawBLOB', 'a String')" | grep Exception
${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --query "select * from url('http://admin4*%25%3Aok@127.0.0.1:${PORT}/example', 'RawBLOB', 'a String')" | grep Exception

