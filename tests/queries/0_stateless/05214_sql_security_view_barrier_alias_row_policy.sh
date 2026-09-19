#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read through an `Alias` table applies the row policies of the `Alias` itself and of its target
# table together (03636). `StorageView::canHideRows` resolves the `Alias` to the storage that serves
# the read, and it must not lose the policy attached to the `Alias` on the way: a projection-only
# `SQL SECURITY DEFINER` view over an `Alias` with a policy defined on the `Alias` only is a
# row-hiding view, so it must stay a barrier - otherwise the invoker's predicate is pushed into the
# source read, granules are skipped by the values of the rows the policy hides, and `read_rows`
# tells the invoker whether such a row exists (04758). A `SQL SECURITY NONE` view is not covered:
# it runs without a user, so no row policy applies to it at all and it hides nothing.

user="user05214_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --allow_experimental_alias_table_engine 1 <<EOSQL
-- \`key\` is the sort key, so it is what the outer predicate would prune on. Enough rows for
-- several granules, so that pruning is visible in \`read_rows\` at all.
CREATE TABLE $db.owned (key UInt64, owner String)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;
INSERT INTO $db.owned SELECT number, 'nobody' FROM numbers(100000);

CREATE TABLE $db.owned_alias ENGINE = Alias('$db', 'owned');

-- The stored query projects every row of the Alias; the policy on the Alias is the only boundary.
CREATE VIEW $db.owned_alias_view
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.owned_alias;

-- The same view without a security context switch, as the optimization baseline.
CREATE VIEW $db.owned_alias_view_invoker
SQL SECURITY INVOKER
AS SELECT * FROM $db.owned_alias;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.owned_alias_view TO $user;
GRANT SELECT ON $db.owned_alias_view_invoker TO $user;
GRANT SELECT ON $db.owned_alias TO $user;
GRANT SELECT ON $db.owned TO $user;

-- The policy is defined on the Alias only, not on its target table, and applies to every reader,
-- so the definer of the view is subject to it too. Every row is owned by somebody else.
CREATE ROW POLICY ${user}_alias ON $db.owned_alias FOR SELECT USING owner = currentUser() TO ALL;
EOSQL

echo "===== the views expose no row ====="
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM $db.owned_alias_view"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM $db.owned_alias_view_invoker"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $db.owned_alias"

echo "===== the definer view keeps the invoker's predicate above the source read ====="
# The INVOKER twin is the baseline: there the predicate reaches the read of the source table. The
# settings pin the plan shape, because the test also runs with randomized settings.
explain_of() {
    ${CLICKHOUSE_CLIENT} --user "$user" --enable_parallel_replicas 0 \
        --query_plan_merge_filters 1 --optimize_move_to_prewhere 0 --query_plan_optimize_prewhere 0 \
        --query "EXPLAIN actions = 0, description = 0 SELECT count() FROM $db.$1 WHERE key = 99999"
}
if diff <(explain_of owned_alias_view) <(explain_of owned_alias_view_invoker) > /dev/null
then echo "same"; else echo "differs"; fi

# `99999` exists in the table but is hidden by the policy; `500000` exists nowhere. With the barrier
# the two must read the same number of rows, so the invoker learns nothing about the hidden row.
#
# The comparison is an exact equality of `read_rows` between two runs, so the per-query random
# read-path injections the test harness enables must be pinned off, and a single thread keeps the
# read pool deterministic - none of them affects the index analysis the test guards.
probe() {
    local query_id="probe_${CLICKHOUSE_DATABASE}_$1"
    ${CLICKHOUSE_CLIENT} --user "$user" --query_id "$query_id" \
        --max_threads 1 \
        --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0 \
        --page_cache_inject_eviction 0 \
        --query "SELECT count() FROM $db.owned_alias_view WHERE key = $1" > /dev/null
    echo "$query_id"
}

echo "===== reading the definer view costs the same whether or not the hidden row matches ====="
hidden_id=$(probe 99999)
absent_id=$(probe 500000)

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
# `count() = 2` guards against the comparison passing vacuously on an empty match.
${CLICKHOUSE_CLIENT} --query "
    SELECT multiIf(
        count() != 2, 'MISSING',
        anyIf(read_rows, query_id = '$hidden_id') = anyIf(read_rows, query_id = '$absent_id'),
        'same', 'DISCLOSED')
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND query_id IN ('$hidden_id', '$absent_id') AND type = 'QueryFinish'"

${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${user}_alias ON $db.owned_alias"
${CLICKHOUSE_CLIENT} --query "DROP USER $user"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.owned_alias_view, $db.owned_alias_view_invoker"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.owned_alias, $db.owned"
