#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
  CREATE TABLE event_envoy
  (
      timestamp_interval DateTime CODEC(DoubleDelta),
      region LowCardinality(String),
      cluster LowCardinality(String)
  )
  ENGINE = MergeTree
  ORDER BY (timestamp_interval)
  SETTINGS index_granularity = 8192;

  INSERT INTO event_envoy SELECT now() - number, 'us-east-1', 'ch_super_fast' FROM numbers_mt(1e5);
"

${CLICKHOUSE_CLIENT} -q "
  CREATE TABLE event_envoy_remote
  (
      timestamp_interval DateTime CODEC(DoubleDelta),
      region LowCardinality(String),
      cluster LowCardinality(String)
  ) AS remote('127.0.0.1', '${CLICKHOUSE_DATABASE}', event_envoy);
"

${CLICKHOUSE_CLIENT} -q "
  CREATE TABLE global_event_envoy
  (
      timestamp_interval DateTime,
      region LowCardinality(String),
      cluster LowCardinality(String)
  )
  ENGINE = Merge('${CLICKHOUSE_DATABASE}', 'event_envoy.*');
"

${CLICKHOUSE_CLIENT} --prefer_localhost_replica 1 -q "
  EXPLAIN indexes=1
   SELECT timestamp_interval
     FROM global_event_envoy
    WHERE timestamp_interval <= now() - 54321 AND region = 'us-east-1'
" | grep -c 'Condition.*timestamp_interval'

