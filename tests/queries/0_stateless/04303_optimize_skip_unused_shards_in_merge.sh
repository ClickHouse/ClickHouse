#!/usr/bin/env bash
# Tags: shard

# Distributed tables wrapped in `Merge` (engine or `merge()` function) must
# still honour `optimize_skip_unused_shards`/`force_optimize_skip_unused_shards`
# when the predicate is applied above the `Merge`. Otherwise the predicate is
# pushed past `Distributed` and both shards are queried, which (for
# self-referential clusters such as `test_cluster_two_shards`) silently doubles
# the result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Use a dedicated, uniquely-named database so the test is safe to run
# concurrently with itself (e.g. under flaky checks).
DB="${CLICKHOUSE_DATABASE}_04303"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB};
CREATE DATABASE ${DB};

CREATE TABLE ${DB}.data1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE ${DB}.data2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO ${DB}.data1 SELECT number,        number * 10 FROM numbers(100);
INSERT INTO ${DB}.data2 SELECT number + 1000, number * 10 FROM numbers(100);

CREATE TABLE ${DB}.dist1 AS ${DB}.data1 ENGINE = Distributed(test_cluster_two_shards, ${DB}, data1, k);
CREATE TABLE ${DB}.dist2 AS ${DB}.data2 ENGINE = Distributed(test_cluster_two_shards, ${DB}, data2, k);

CREATE TABLE ${DB}.merge_dist AS ${DB}.data1 ENGINE = Merge(${DB}, '^dist[12]\$');

-- View wrapping a \`Distributed\` table; covers the \`Merge\` → \`View\` → \`Distributed\` chain.
CREATE VIEW ${DB}.view1 AS SELECT * FROM ${DB}.dist1;
CREATE TABLE ${DB}.merge_view AS ${DB}.data1 ENGINE = Merge(${DB}, '^view1\$');

SET enable_analyzer = 1;
SET optimize_skip_unused_shards = 1;

-- Baseline: a direct \`Distributed\` query prunes the unused shard.
SELECT 'direct', count() FROM ${DB}.dist1 WHERE k = 5;
SELECT 'direct', count() FROM ${DB}.dist1 WHERE k = 5 SETTINGS force_optimize_skip_unused_shards = 2;

-- \`merge()\` table function over two \`Distributed\` tables: predicate must reach
-- the sharding-key analysis of each \`Distributed\` child so that only one shard
-- is queried (count = 1, not 2).
SELECT 'merge_func', count() FROM merge(${DB}, '^dist[12]\$') WHERE k = 5;
SELECT 'merge_func', count() FROM merge(${DB}, '^dist[12]\$') WHERE k = 5 SETTINGS force_optimize_skip_unused_shards = 2;

-- Same via the \`Merge\` table engine.
SELECT 'merge_engine', count() FROM ${DB}.merge_dist WHERE k = 5;
SELECT 'merge_engine', count() FROM ${DB}.merge_dist WHERE k = 5 SETTINGS force_optimize_skip_unused_shards = 2;

-- \`Merge\` over a \`View\` that itself wraps a \`Distributed\`: the planner must
-- still trigger filter analysis so that the inner \`Distributed\` can prune.
SELECT 'merge_view', count() FROM ${DB}.merge_view WHERE k = 5;
SELECT 'merge_view', count() FROM ${DB}.merge_view WHERE k = 5 SETTINGS force_optimize_skip_unused_shards = 2;
SELECT 'merge_view_func', count() FROM merge(${DB}, '^view1\$') WHERE k = 5;
SELECT 'merge_view_func', count() FROM merge(${DB}, '^view1\$') WHERE k = 5 SETTINGS force_optimize_skip_unused_shards = 2;

DROP DATABASE ${DB};
"
