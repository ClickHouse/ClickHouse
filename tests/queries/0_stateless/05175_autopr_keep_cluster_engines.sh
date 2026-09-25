#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: `automatic_parallel_replicas_mode` is implemented only in the analyzer.

# `automatic_parallel_replicas_mode` decides only for `MergeTree` reads. A cluster engine (`url`, `s3`,
# a table of a data lake catalog, ...) must keep using parallel replicas when they are enabled, instead of
# reading everything on the initiator. The reads below are only planned, nothing listens on port 1.
# `EXPLAIN` is not wrapped into a `SELECT`: a table function under `SELECT ... FROM (EXPLAIN ...)` is never
# replaced by its cluster alternative.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SETTINGS="parallel_replicas_for_cluster_engines = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_mode = 'read_tasks', parallel_replicas_only_with_analyzer = 1"
QUERY="SELECT sum(x) FROM url('http://localhost:1/05175.tsv', TSV, 'x UInt64')"

function read_step()
{
    $CLICKHOUSE_CLIENT --multiquery -q "$1" | grep -o 'ReadFrom[A-Za-z]*'
}

echo "settings clause, mode 1: $(read_step "EXPLAIN $QUERY SETTINGS $SETTINGS, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1")"
echo "settings clause, mode 2: $(read_step "EXPLAIN $QUERY SETTINGS $SETTINGS, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2")"

# The settings of the session are not re-applied from the query like a `SETTINGS` clause.
echo "session settings: $(read_step "SET $SETTINGS, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1; EXPLAIN $QUERY")"

# The automatic mode alone does not enable parallel replicas.
echo "automatic mode only: $(read_step "SET $SETTINGS, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 1; EXPLAIN $QUERY")"
