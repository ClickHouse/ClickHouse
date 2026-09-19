#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `additional_table_filters` entry keyed to a `SQL SECURITY DEFINER` / `NONE` view is applied as a
# security barrier `Filter` step above the view. Two plan rewrites rebuild such a step: with
# `query_plan_lower_array_join_function` an `arrayJoin` inside the filter is lowered into
# `Expression -> ArrayJoin -> Filter`, and `query_plan_fuse_filter_into_array_join` then moves the
# remaining filter into the `ArrayJoin` step and leaves a pass-through `Expression` behind. The
# replacement steps used to lose the barrier flag, so the invoker's `WHERE` merged into the rebuilt
# filter and was pushed below the `ArrayJoin`, where it observed the rows the barrier filter hides.

db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.tagged (key UInt64, tags Array(String)) ENGINE = MergeTree ORDER BY key;
INSERT INTO $db.tagged VALUES (1, ['public']), (2, ['private']), (3, ['public', 'other']), (4, []);

CREATE VIEW $db.tagged_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT key, tags FROM $db.tagged;
CREATE VIEW $db.tagged_none SQL SECURITY NONE AS SELECT key, tags FROM $db.tagged;
CREATE VIEW $db.tagged_invoker SQL SECURITY INVOKER AS SELECT key, tags FROM $db.tagged;
EOSQL

# The settings pin the plan shape the assertions below depend on. Without short-circuit evaluation a
# merged filter computes every conjunct on every row, so `throwIf` fires as soon as the invoker's
# predicate shares a step with, or runs below, the barrier filter.
client="${CLICKHOUSE_CLIENT} --enable_parallel_replicas 0 --max_threads 1
    --query_plan_lower_array_join_function 1 --query_plan_filter_push_down 1 --query_plan_merge_expressions 1
    --query_plan_merge_filters 1 --short_circuit_function_evaluation disable --serialize_query_plan 0
    --query_plan_max_step_description_length 10000"

filter="additional_table_filters = {'$db.VIEW': 'arrayJoin(tags) = ''public'''}"

for fuse in 0 1; do
    echo "===== fuse_filter_into_array_join = $fuse: the invoker's predicate stays above the barrier ====="
    # The rewritten barrier filter must not share a step with the outer `WHERE`: a merged step lists
    # both descriptions. The `INVOKER` view is the positive control proving that the oracle sees the merge.
    for view in tagged_invoker tagged_definer tagged_none; do
        ${client} --query_plan_fuse_filter_into_array_join "$fuse" --query \
            "SELECT '$view', countIf(explain LIKE '%WHERE%' AND explain ILIKE '%additional filter%')
             FROM (EXPLAIN actions = 0, description = 1
                   SELECT count() FROM $db.$view WHERE key != 42 SETTINGS ${filter/VIEW/$view})"
    done

    echo "===== fuse_filter_into_array_join = $fuse: the invoker's predicate cannot observe a hidden row ====="
    # The exception is matched by its code name and not by the message, because the client echoes
    # the query text on failure.
    for view in tagged_invoker tagged_definer tagged_none; do
        output=$(${client} --query_plan_fuse_filter_into_array_join "$fuse" --query \
            "SELECT count() FROM $db.$view WHERE throwIf(key = 2, 'DISCLOSED') = 0 SETTINGS ${filter/VIEW/$view}" 2>&1)
        if grep -q FUNCTION_THROW_IF_VALUE_IS_NON_ZERO <<< "$output"; then
            echo "$view: the outer predicate saw a hidden row"
        else
            echo "$view: $output"
        fi
    done
done

echo "===== the lowered filter still hides the same rows ====="
${client} --query \
    "SELECT key FROM $db.tagged_definer WHERE key != 42 ORDER BY key SETTINGS ${filter/VIEW/tagged_definer}"
