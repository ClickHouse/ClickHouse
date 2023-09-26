#!/usr/bin/env bash
# Tags: long, no-ubsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCRIPT_PATH="$CURDIR/$CLICKHOUSE_TEST_UNIQUE_NAME.generated-expect"

# NOTE: database = $CLICKHOUSE_DATABASE is superfluous

function test_completion_word()
{
    local w=$1 && shift

    local w_len=${#w}
    local compword_begin=${w:0:$((w_len-3))}
    local compword_end=${w:$((w_len-3))}

    # NOTE:
    # - here and below you should escape variables of the expect.
    # - you should not use "expect <<..." since in this case timeout/eof will
    #   not work (I guess due to attached stdin)
    cat > "$SCRIPT_PATH" << EOF
# NOTE: log will be appended
exp_internal -f $CLICKHOUSE_TMP/$(basename "${BASH_SOURCE[0]}").debuglog 0

# NOTE: when expect have EOF on stdin it also closes stdout, so let's reopen it
# again for logging
set stdout_channel [open "/dev/stdout" w]

log_user 0
set timeout 60
match_max 100000
expect_after {
    # Do not ignore eof from expect
    -i \$any_spawn_id eof { exp_continue }
    # A default timeout action is to do nothing, change it to fail
    -i \$any_spawn_id timeout { exit 1 }
}

spawn bash -c "$*"
expect ":) "

# Make a query
send -- "SET $compword_begin"
expect "SET $compword_begin"

# Wait for suggestions to load, they are loaded in background
set is_done 0
set timeout 1
while {\$is_done == 0} {
    send -- "\\t"
    expect {
        "$compword_begin$compword_end" {
            puts \$stdout_channel "$compword_begin$compword_end: OK"
            set is_done 1
        }
        default {
            sleep 1
        }
    }
}

close \$stdout_channel

send -- "\\3\\4"
expect eof
EOF

    # NOTE: run expect under timeout since there is while loop that is not
    # limited with timeout.
    #
    # NOTE: cat is required to serialize stdout for expect (without this pipe
    # it will reopen the file again, and the output will be mixed).
    timeout 2m expect -f "$SCRIPT_PATH" | cat
}

# last 3 bytes will be completed,
# so take this in mind when you will update the list.
client_compwords_positive=(
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
    # system.settings
    max_concurrent_queries_for_all_users
    # system.clusters
    test_shard_localhost
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

local_compwords_positive=(
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
)

echo "# clickhouse-client"
for w in "${client_compwords_positive[@]}"; do
    test_completion_word "$w" "$CLICKHOUSE_CLIENT"
done
echo "# clickhouse-local"
for w in "${local_compwords_positive[@]}"; do
    test_completion_word "$w" "$CLICKHOUSE_LOCAL"
done

rm -f "${SCRIPT_PATH:?}"

exit 0
