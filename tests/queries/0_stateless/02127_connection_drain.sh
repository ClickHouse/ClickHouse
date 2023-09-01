#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# sync drain
for _ in {1..100}; do
    prev=$(curl -d@- -sS "${CLICKHOUSE_URL}" <<<"select value from system.metrics where metric = 'SyncDrainedConnections'")
    curl -d@- -sS "${CLICKHOUSE_URL}" <<<"select * from remote('127.{2,3}', view(select * from numbers(1e6))) limit 100 settings drain_timeout=-1 format Null"
    now=$(curl -d@- -sS "${CLICKHOUSE_URL}" <<<"select value from system.metrics where metric = 'SyncDrainedConnections'")
    if [[ "$prev" != $(( now-2 )) ]]; then
        continue
    fi
    echo "OK: sync drain"
    break
done

# async drain
for _ in {1..100}; do
    prev=$(curl -d@- -sS "${CLICKHOUSE_URL}" <<<"select value from system.metrics where metric = 'AsyncDrainedConnections'")
    curl -d@- -sS "${CLICKHOUSE_URL}" <<<"select * from remote('127.{2,3}', view(select * from numbers(1e6))) limit 100 settings drain_timeout=10 format Null"
    now=$(curl -d@- -sS "${CLICKHOUSE_URL}" <<<"select value from system.metrics where metric = 'AsyncDrainedConnections'")
    if [[ "$prev" != $(( now-2 )) ]]; then
        continue
    fi
    echo "OK: async drain"
    break
done
