#!/usr/bin/env bash
# Tags: shard

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `_shard_num` is shipped by the initiator of a distributed query, while the view's own SETTINGS name a
# different `cluster_for_parallel_replicas`. Applying that shard number to the view's cluster used to
# throw "Shard number is greater than shard count" (shard 2 of a 1-shard cluster).

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_04727 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04727 SELECT number FROM numbers(100);

CREATE VIEW v_04727 AS SELECT a FROM t_04727
  SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
           parallel_replicas_for_non_replicated_merge_tree = 1,
           automatic_parallel_replicas_mode = 0,
           cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
"

# prefer_localhost_replica=0 makes both shards read over the network, which is what ships _shard_num.
# Each shard of test_cluster_two_shards_localhost points at this same server, so the expected answer is
# the view's sum counted twice.
$CLICKHOUSE_CLIENT -q "
SELECT sum(a) FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), v_04727)
SETTINGS prefer_localhost_replica = 0;
"

$CLICKHOUSE_CLIENT -q "DROP VIEW v_04727; DROP TABLE t_04727;"
