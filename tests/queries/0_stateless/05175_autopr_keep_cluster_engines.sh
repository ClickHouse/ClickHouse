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

# The compatibility checks of parallel replicas apply to cluster engines in every mode, e.g. an `IN` subquery in force mode.
for mode in 0 1 2; do
    echo "IN subquery in force mode, mode $mode: $($CLICKHOUSE_CLIENT -q "EXPLAIN $QUERY WHERE x IN (SELECT 1) SETTINGS $SETTINGS,
        enable_parallel_replicas = 2, automatic_parallel_replicas_mode = $mode, parallel_replicas_allow_in_with_subquery = 0" 2>&1 \
        | grep -o -m1 'SUPPORT_IS_DISABLED')"
done

# A replica would not apply `additional_table_filters` to the query shipped by a cluster engine (the table expression
# is renamed there), so a cluster engine is not replaced by its `*Cluster` variant with them, in any mode and whether
# or not the plan is serialized. The read goes to this server: `sum(n)` over `n > 1` of 0..3 is 5.
FILTERED_QUERY="SELECT sum(n) FROM url('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n+FROM+numbers(4)', TSV, 'n UInt64') AS t"
for serialize in 0 1; do
    for mode in 0 1 2; do
        FILTER_SETTINGS="$SETTINGS, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = $mode,
            serialize_query_plan = $serialize, additional_table_filters = {'t': 'n > 1'}"
        echo "additional_table_filters, serialize_query_plan $serialize, mode $mode:" \
            "$(read_step "EXPLAIN $FILTERED_QUERY SETTINGS $FILTER_SETTINGS")" \
            "$($CLICKHOUSE_CLIENT -q "$FILTERED_QUERY SETTINGS $FILTER_SETTINGS")"
    done
done

# In force mode without `serialize_query_plan` the combination is rejected, as for `MergeTree`, in every mode.
for mode in 0 1 2; do
    echo "additional_table_filters in force mode, mode $mode: $($CLICKHOUSE_CLIENT -q "EXPLAIN $FILTERED_QUERY SETTINGS $SETTINGS,
        enable_parallel_replicas = 2, automatic_parallel_replicas_mode = $mode, serialize_query_plan = 0,
        additional_table_filters = {'t': 'n > 1'}" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED')"
done
