#!/usr/bin/env bash
# Tags: distributed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We check that even if max_threads is small, the setting max_distributed_connections
# will allow to process queries on multiple shards concurrently.

# We do sleep 1.5 seconds on ten machines.
# If concurrency is one (bad) the query will take at least 15 seconds and the following loops are guaranteed to be infinite.
# If concurrency is 10 (good), the query may take less than 10 second with non-zero probability
#  and the following loops will finish with probability 1 assuming independent random variables.

i=0 retries=30
while [[ $i -lt $retries ]]; do
    timeout 10 ${CLICKHOUSE_CLIENT} --max_threads 1 --max_distributed_connections 10 --query "
        SELECT sleep(1.5) FROM remote('127.{1..10}', system.one) FORMAT Null" --prefer_localhost_replica=0 && break
    ((++i))
done

i=0 retries=30
while [[ $i -lt $retries ]]; do
    timeout 10 ${CLICKHOUSE_CLIENT} --max_threads 1 --max_distributed_connections 10 --query "
        SELECT sleep(1.5) FROM remote('127.{1..10}', system.one) FORMAT Null" --prefer_localhost_replica=1 && break
    ((++i))
done

# If max_distributed_connections is low and async_socket_for_remote is disabled,
#  the concurrency of distributed queries will be also low.

timeout 1 ${CLICKHOUSE_CLIENT} --max_threads 1 --max_distributed_connections 1 --async_socket_for_remote 0 --query "
    SELECT sleep(0.15) FROM remote('127.{1..10}', system.one) FORMAT Null" --prefer_localhost_replica=0 && echo 'Fail'

timeout 1 ${CLICKHOUSE_CLIENT} --max_threads 1 --max_distributed_connections 1 --async_socket_for_remote 0 --query "
    SELECT sleep(0.15) FROM remote('127.{1..10}', system.one) FORMAT Null" --prefer_localhost_replica=1 && echo 'Fail'

echo 'Ok'
