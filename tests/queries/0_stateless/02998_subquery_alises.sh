#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function get_query() {
  alias_name=$1

  query="WITH a AS
    (
        SELECT sumIf(dummy, dummy IN (select dummy from system.one)) as $alias_name
        FROM $CLICKHOUSE_DATABASE.dist_one
    )
  SELECT
      sum(dummy),
      sumIf(dummy, dummy IN (a))
  FROM dist_one settings distributed_product_mode='allow'"
  echo "$query"
}

$CLICKHOUSE_CLIENT -q "drop table if exists dist_one"
$CLICKHOUSE_CLIENT -q "create table dist_one (dummy UInt8) engine Distributed('test_cluster_two_shards', system, one)"
# Getting counter for auto-generated subquery alias
# NOTE: no need to execute full query, find out current counter for subquery is enough
last_alias_counter=$($CLICKHOUSE_CLIENT -q "EXPLAIN SYNTAX $(get_query 'test_alias')" | grep -F _subquery | sed -E 's/^.*_subquery([0-9]+).*$/\1/')
# NOTE: query analysis done multiple times
new_alias_counter=$((last_alias_counter + 5))
new_query=$(get_query "_subquery$new_alias_counter")
$CLICKHOUSE_CLIENT -q "$new_query"
