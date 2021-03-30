#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function test_completion_word()
{
    local w=$1 && shift

    local w_len=${#w}
    local compword_begin=${w:0:$((w_len-3))}
    local compword_end=${w:$((w_len-3))}

    # NOTE: here and below you should escape variables of the expect.
    timeout 22s expect << EOF
log_user 0
set timeout 3
match_max 100000
# A default timeout action is to do nothing, change it to fail
expect_after {
    timeout {
        exit 1
    }
}

spawn bash -c "$CLICKHOUSE_CLIENT_BINARY $CLICKHOUSE_CLIENT_OPT"
expect ":) "

# Make a query
send -- "SET $compword_begin"
expect "SET $compword_begin"

# Wait for suggestions to load, they are loaded in background
set is_done 0
while {\$is_done == 0} {
    send -- "\\t"
    expect {
        "$compword_begin$compword_end" {
            set is_done 1
        }
        default {
            sleep 1
        }
    }
}

send -- "\\3\\4"
expect eof
EOF
}

# last 3 bytes will be completed,
# so take this in mind when you will update the list.
compwords_positive=(
    # system.functions
    concatAssumeInjective
    # system.table_engines
    ReplacingMergeTree
    # system.formats
    JSONEachRow
    # system.table_functions
    clusterAllReplicas
    # system.data_type_families
    SimpleAggregateFunction
    # system.merge_tree_settings
    write_ahead_log_interval_ms_to_fsync
    # system.settings
    max_concurrent_queries_for_all_users
    # system.clusters
    test_shard_localhost
    # system.errors, also it is very rare to cover system_events_show_zero_values
    CONDITIONAL_TREE_PARENT_NOT_FOUND
    # system.events, also it is very rare to cover system_events_show_zero_values
    WriteBufferFromFileDescriptorWriteFailed
    # system.asynchronous_metrics, also this metric has zero value
    #
    # NOTE: that there is no ability to complete metrics like
    # jemalloc.background_thread.num_runs, due to "." is used as a word breaker
    # (and this cannot be changed -- db.table)
    ReplicasMaxAbsoluteDelay
    # system.metrics
    PartsPreCommitted
    # system.macros
    default_path_test
    # system.storage_policies, egh not uniq
    default
    # system.aggregate_function_combinators
    uniqCombined64ForEach

    # FIXME: one may add separate case for suggestion_limit
    # system.databases
    system
    # system.tables
    aggregate_function_combinators
    # system.columns
    primary_key_bytes_in_memory_allocated
    # system.dictionaries
    # FIXME: none
)
for w in "${compwords_positive[@]}"; do
    test_completion_word "$w" || echo "[FAIL] $w (positive)"
done

# One negative is enough
compwords_negative=(
    # system.clusters
    test_shard_localhost_no_such_cluster
)
for w in "${compwords_negative[@]}"; do
    test_completion_word "$w" && echo "[FAIL] $w (negative)"
done

exit 0
