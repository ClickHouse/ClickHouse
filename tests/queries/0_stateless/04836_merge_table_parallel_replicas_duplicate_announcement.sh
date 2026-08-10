#!/usr/bin/env bash
# When a `merge()` table function's regex matches the table the outer query reads, the outer read
# and the child read derive the identical parallel replicas `stream_id`, and without the
# `enable_parallel_replicas` clear in `ReadFromMerge::createChildrenPlans` one follower builds
# several read pools under one `replica_num` and announces more than once into the same
# coordinator. Depending on follower interleaving the coordinator then threw either
# `Initiator received more initial requests than there are replicas` or
# `Duplicate announcement received for replica number N`.
#
# The table must live in the `default` database: a single-argument `merge()` resolves against the
# default database on each hop, so under a per-test database the follower fails with
# `CANNOT_EXTRACT_TABLE_STRUCTURE` before it ever announces. Because `default` is shared by every
# concurrently running test, the table name carries `$CLICKHOUSE_DATABASE` so that two runs of this
# test - the flaky check runs it many times at once - cannot drop or create each other's table. The
# regex is anchored to that unique name so no other table can leak into the merge.
# `index_granularity = 128` matters: at the default granularity the initiator's replica claims every
# mark range and the followers are cancelled before announcing, so the collision never fires.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

table="t_merge_pr_dup_announce_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS default.${table};

CREATE TABLE default.${table} (dt DateTime, idx Int32, i Nullable(UInt64))
ENGINE = MergeTree PARTITION BY (idx % 3) ORDER BY idx SETTINGS index_granularity = 128;

INSERT INTO default.${table} SELECT toDateTime(number), number, number FROM numbers(500000);
"

$CLICKHOUSE_CLIENT --query "
USE default;

SET enable_analyzer = 1;
SET allow_experimental_parallel_reading_from_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_min_number_of_rows_per_replica = 0;

SET parallel_replicas_local_plan = 0;
SELECT count() FROM default.${table}
WHERE idx GLOBAL IN (i NOT IN (SELECT i FROM merge('^${table}\$') WHERE dt >= -2147483648));

SET parallel_replicas_local_plan = 1;
SELECT count() FROM default.${table}
WHERE idx GLOBAL IN (i NOT IN (SELECT i FROM merge('^${table}\$') WHERE dt >= -2147483648));
"

$CLICKHOUSE_CLIENT --query "DROP TABLE default.${table}"
