#!/usr/bin/env bash
# Tags: distributed

# `ClientInfo` of an async `Distributed` insert is persisted in the queue file and forwarded to the
# shard. A query id longer than 64 KiB is accepted over HTTP, so the server must be able to read back
# and forward what it wrote itself.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE data (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE dist AS data ENGINE = Distributed(test_shard_localhost, currentDatabase(), data);
    SYSTEM STOP DISTRIBUTED SENDS dist;
"

query_id="$(printf 'q%.0s' {1..100000})_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${query_id}&distributed_foreground_insert=0&prefer_localhost_replica=0" \
    -d "INSERT INTO dist VALUES (1), (2), (3)"

${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH DISTRIBUTED dist;
    SELECT count(), sum(x) FROM data;
    DROP TABLE dist;
    DROP TABLE data;
"
