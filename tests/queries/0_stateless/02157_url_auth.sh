#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Not sure why fail even in sequential mode. Disabled for now to make some progress.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test
PORT=33339
PID=$(lsof -i:${PORT} |awk '{print $2}'| tail -1)
if [ -n "${PID}" ]; then
    kill -9 "$PID"
fi

python3 "$CURDIR"/02157_url_auth.python $PORT &

sleep 1

${CLICKHOUSE_CLIENT} --query "select * from url('http://admin1:password@127.0.0.1:33339/example', 'RawBLOB', 'a String')" | grep Exception
${CLICKHOUSE_CLIENT} --query "select * from url('http://admin2:password%2F@127.0.0.1:33339/example', 'RawBLOB', 'a String')" | grep Exception
${CLICKHOUSE_CLIENT} --query "select * from url('http://admin3%3F%2F%3APassWord%5E%23%3F%2F@127.0.0.1:33339/example', 'RawBLOB', 'a String')" | grep Exception
${CLICKHOUSE_CLIENT} --query "select * from url('http://admin4*%25%3Aok@127.0.0.1:33339/example', 'RawBLOB', 'a String')" | grep Exception

