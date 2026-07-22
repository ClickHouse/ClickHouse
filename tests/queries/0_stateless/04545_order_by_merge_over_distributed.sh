#!/usr/bin/env bash
# Tags: shard

# Global ORDER BY must be applied across shards when a Distributed table is read through a
# Merge engine. Before the fix, ReadFromMerge narrowed (concatenated) the per-shard-sorted
# streams at max_threads = 1, so the outer merge-only sort had a single input and LIMIT
# returned one shard's local top rows instead of the global top rows (issue #111211).
#
# Parallel-safe: the two shard tables get a per-test unique name in the shared shard_0/shard_1
# databases, and the Distributed/Merge tables live in the per-test database. The shared
# databases are only created (never dropped), so concurrent copies of this test do not collide.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

t="t_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;
DROP TABLE IF EXISTS shard_0.${t} SYNC;
DROP TABLE IF EXISTS shard_1.${t} SYNC;
CREATE TABLE shard_0.${t} (w Int64) ENGINE = MergeTree ORDER BY w;
CREATE TABLE shard_1.${t} (w Int64) ENGINE = MergeTree ORDER BY w;
INSERT INTO shard_0.${t} SELECT number * 3 FROM numbers(100);        -- max 297
INSERT INTO shard_1.${t} SELECT number * 3 + 1000 FROM numbers(100); -- max 1297
CREATE TABLE dist AS shard_0.${t} ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ${t});
CREATE TABLE merge_t AS dist ENGINE = Merge(currentDatabase(), '^dist\$');
"

# max_threads = 1 is what makes narrowPipe collapse the two per-shard streams into one.
run() { ${CLICKHOUSE_CLIENT} --max_threads 1 --query "$1"; }

echo 'analyzer, DESC'
run "SELECT w FROM merge_t ORDER BY w DESC LIMIT 3 SETTINGS enable_analyzer = 1"
echo 'analyzer, ASC'
run "SELECT w FROM merge_t ORDER BY w ASC LIMIT 3 SETTINGS enable_analyzer = 1"
echo 'old analyzer, DESC'
run "SELECT w FROM merge_t ORDER BY w DESC LIMIT 3 SETTINGS enable_analyzer = 0"

# After-aggregation stages also use a merge-only sort.
echo 'analyzer, group by + DESC, no_merge=2'
run "SELECT w FROM merge_t GROUP BY w ORDER BY w DESC LIMIT 3 SETTINGS enable_analyzer = 1, distributed_group_by_no_merge = 2"
echo 'old analyzer, group by + DESC, no_merge=2'
run "SELECT w FROM merge_t GROUP BY w ORDER BY w DESC LIMIT 3 SETTINGS enable_analyzer = 0, distributed_group_by_no_merge = 2"
echo 'analyzer, group by + DESC, no_merge=2, push_down_limit=0'
run "SELECT w FROM merge_t GROUP BY w ORDER BY w DESC LIMIT 3 SETTINGS enable_analyzer = 1, distributed_group_by_no_merge = 2, distributed_push_down_limit = 0"

# Order-sensitive head/tail: the top rows come from shard 1, the bottom from shard 0,
# so a broken global sort changes these boundary rows (min/max/count would not).
echo 'full order head'
run "SELECT * FROM (SELECT w FROM merge_t ORDER BY w DESC LIMIT 2) SETTINGS enable_analyzer = 1"
echo 'full order tail'
run "SELECT * FROM (SELECT w FROM merge_t ORDER BY w ASC LIMIT 2) SETTINGS enable_analyzer = 1"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE merge_t;
DROP TABLE dist;
DROP TABLE shard_0.${t} SYNC;
DROP TABLE shard_1.${t} SYNC;
"
