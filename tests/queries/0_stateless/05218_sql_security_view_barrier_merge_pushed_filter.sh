#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `WHERE` of a `SQL SECURITY DEFINER` / `NONE` view over a `Merge` table is a security barrier
# step. `tryPushDownFilter` sinks such a filter into `ReadFromMerge`, which re-creates it inside every
# child plan and optimizes those plans separately. The barrier must travel with the filter: the
# re-created filter steps and the `ReadFromMerge` step itself carry the flag, so that nothing the
# invoker wrote - the outer `WHERE`, an `additional_result_filter` of the invoker's session - is
# evaluated on the rows the view hides, and the index analysis of the source tables never sees the
# invoker's predicate (`read_rows` would otherwise tell the invoker whether a hidden row exists).

user="user05218_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOSQL
-- \`key\` is the sort key, so it is what the outer predicate would prune on. Enough rows for
-- several granules, so that pruning is visible in \`read_rows\` at all.
CREATE TABLE $db.owned (key UInt64, owner String)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;
INSERT INTO $db.owned SELECT number, 'nobody' FROM numbers(100000);

CREATE TABLE $db.owned_merge ENGINE = Merge('$db', '^owned\$');

-- Expose nothing to the invoker: every row is owned by somebody else.
CREATE VIEW $db.owned_merge_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT key, owner FROM $db.owned_merge WHERE owner = 'visible';

CREATE VIEW $db.owned_merge_none SQL SECURITY NONE
AS SELECT key, owner FROM $db.owned_merge WHERE owner = 'visible';

-- The optimization baseline: the invoker's predicate does reach the source table here.
CREATE VIEW $db.owned_merge_invoker SQL SECURITY INVOKER
AS SELECT key, owner FROM $db.owned_merge WHERE owner = 'visible';

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.owned_merge_definer TO $user;
GRANT SELECT ON $db.owned_merge_none TO $user;
GRANT SELECT ON $db.owned_merge_invoker TO $user;
GRANT SELECT ON $db.owned_merge TO $user;
GRANT SELECT ON $db.owned TO $user;
-- \`EXPLAIN\` in a subquery needs it.
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
EOSQL

echo "===== the views expose no row ====="
for view in owned_merge_definer owned_merge_none owned_merge_invoker; do
    ${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM $db.$view"
done

echo "===== the invoker's predicate stays out of the source read of the barrier views ====="
# The view's own `WHERE` is pushed into the `Merge` child plan and becomes its PREWHERE; the outer
# predicate may join it only for the INVOKER twin, which is what makes this oracle non-vacuous.
# The control depends on the prewhere optimizations being on, and the test harness randomizes them:
# without `query_plan_merge_filters` the two predicates stay separate steps and only the view's own
# one moves; without `optimize_move_to_prewhere` / `query_plan_optimize_prewhere` nothing moves at
# all; `enable_multiple_prewhere_read_steps` splits the moved conjunction into several
# `Prewhere filter column` lines. Pin them, and report whether the predicate got there at all rather
# than how many lines mention it.
for view in owned_merge_definer owned_merge_none owned_merge_invoker; do
    for analyzer in 1 0; do
        pushed=$(${CLICKHOUSE_CLIENT} --user "$user" --enable_analyzer "$analyzer" --enable_parallel_replicas 0 \
            --optimize_move_to_prewhere 1 --query_plan_optimize_prewhere 1 --query_plan_merge_filters 1 \
            --enable_multiple_prewhere_read_steps 0 \
            --query "SELECT countIf(explain LIKE '%Prewhere filter column%' AND explain LIKE '%key%') > 0
                     FROM (EXPLAIN actions = 1 SELECT count() FROM $db.$view WHERE key = 99999)")
        echo -e "$view (enable_analyzer = $analyzer)\tthe invoker's predicate in PREWHERE: $pushed"
    done
done

# `99999` exists in the table but is hidden by the view; `500000` exists nowhere. With the barrier the
# two must read the same number of rows, so the invoker learns nothing about the hidden row.
#
# The comparison is an exact equality of `read_rows` between two runs, so the per-query random
# read-path injections the test harness enables must be pinned off, a single thread keeps the read
# pool deterministic, and the query condition cache is off - it remembers that the view's own
# `WHERE` matches no granule after the first run and would make the second run read nothing.
probe() {
    local query_id="probe_${user}_$1_$2_$3"
    ${CLICKHOUSE_CLIENT} --enable_analyzer "$2" --user "$user" --query_id "$query_id" \
        --max_threads 1 --use_query_condition_cache 0 \
        --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0 \
        --page_cache_inject_eviction 0 \
        --query "SELECT count() FROM $db.$1 WHERE key = $3" > /dev/null
    echo "$query_id"
}

echo "===== reading a barrier view costs the same whether or not the hidden row matches ====="
for view in owned_merge_definer owned_merge_none; do
    for analyzer in 1 0; do
        hidden_id=$(probe "$view" "$analyzer" 99999)
        absent_id=$(probe "$view" "$analyzer" 500000)

        ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
        # `count() = 2` guards against the comparison passing vacuously on an empty match.
        ${CLICKHOUSE_CLIENT} --query "
            SELECT '$view (enable_analyzer = $analyzer)', multiIf(
                count() != 2, 'MISSING',
                anyIf(read_rows, query_id = '$hidden_id') = anyIf(read_rows, query_id = '$absent_id'),
                'same', 'DISCLOSED')
            FROM system.query_log
            WHERE current_database = currentDatabase()
              AND query_id IN ('$hidden_id', '$absent_id') AND type = 'QueryFinish'"
    done
done

echo "===== an additional_result_filter of the invoker never sees a hidden row ====="
# The setting adds a filter step on top of the view's inner query too (it runs at subquery depth 0),
# where it sits right above the view's `WHERE`. It must not be evaluated below it: `throwIf` fires on
# the hidden rows only if it is. Grepping the error code name of the captured output is the oracle,
# because the client echoes the query text (which contains the marker) into the same output. The
# outer query projects `owner` so that the setting is well-formed for the outer query as well; it
# returns no row, so a run that sees nothing hidden prints nothing.
for view in owned_merge_definer owned_merge_none; do
    for analyzer in 1 0; do
        output=$(${CLICKHOUSE_CLIENT} --user "$user" --enable_analyzer "$analyzer" --query "
            SELECT owner FROM $db.$view
            SETTINGS additional_result_filter = 'throwIf(owner = ''nobody'', ''hidden row seen'')'" 2>&1)
        if echo "$output" | grep -q FUNCTION_THROW_IF_VALUE_IS_NON_ZERO
        then echo "$view (enable_analyzer = $analyzer): DISCLOSED"
        else echo "$view (enable_analyzer = $analyzer): no hidden row seen${output:+, output: $output}"; fi
    done
done

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.owned_merge_definer, $db.owned_merge_none, $db.owned_merge_invoker"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.owned_merge, $db.owned"
