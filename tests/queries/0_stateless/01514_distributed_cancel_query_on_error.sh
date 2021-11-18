#!/usr/bin/env bash
# Tags: distributed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# _shard_num:
#   1 on 127.2
#   2 on 127.3
# max_block_size to fail faster
# max_memory_usage/_shard_num/repeat() will allow failure on the first shard earlier.
opts=(
    "--max_memory_usage=1G"
    "--max_block_size=50"
    "--max_threads=1"
    "--max_distributed_connections=2"
)
${CLICKHOUSE_CLIENT} "${opts[@]}" -q "SELECT groupArray(repeat('a', if(_shard_num == 2, 100000, 1))), number%100000 k from remote('127.{2,3}', system.numbers) GROUP BY k LIMIT 10e6" |& {
    # the query should fail earlier on 127.3 and 127.2 should not even go to the memory limit exceeded error.
    grep -F -q 'DB::Exception: Received from 127.3:9000. DB::Exception: Memory limit (for query) exceeded:'
    # while if this will not correctly then it will got the exception from the 127.2:9000 and fail
}
