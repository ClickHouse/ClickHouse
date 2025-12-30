#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists 03444_local;
    drop table if exists 03444_distr;

    create table 03444_local (id UInt32) engine = MergeTree order by id as select * from numbers(100);
    create table 03444_distr (id UInt32) engine = Distributed(test_cluster_one_shard_two_replicas, currentDatabase(), 03444_local);
"

for use_hedged_requests in {0..1}; do
    for async_socket_for_remote in {0..1}; do
        echo "-- use_hedged_requests = $use_hedged_requests, async_socket_for_remote = $async_socket_for_remote"

        $CLICKHOUSE_CLIENT -m -q "
            select 'remote() 1 shard: ' || count()
            from remote('127.0.0.1|127.0.0.2|127.0.0.3', currentDatabase(), 03444_local)
            settings prefer_localhost_replica = 0,
                     max_parallel_replicas = 100,
                     use_hedged_requests = $use_hedged_requests,
                     async_socket_for_remote = $async_socket_for_remote;

            select 'remote() 3 shards: ' || count()
            from remote('127.0.0.1|127.0.0.2,127.0.0.3|127.0.0.4,127.0.0.5|127.0.0.6', currentDatabase(), 03444_local)
            settings prefer_localhost_replica = 0,
                     max_parallel_replicas = 100,
                     use_hedged_requests = $use_hedged_requests,
                     async_socket_for_remote = $async_socket_for_remote;

            select 'Distributed: ' || count()
            from 03444_distr
            settings prefer_localhost_replica = 0,
                     max_parallel_replicas = 100,
                     use_hedged_requests = $use_hedged_requests,
                     async_socket_for_remote = $async_socket_for_remote;
        "

    done
done

$CLICKHOUSE_CLIENT -m -q "
    drop table 03444_local;
    drop table 03444_distr;
"
