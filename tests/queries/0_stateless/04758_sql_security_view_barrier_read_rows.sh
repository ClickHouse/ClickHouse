#!/usr/bin/env bash
# A `SQL SECURITY DEFINER` view is an optimization barrier for index analysis too, not only for the
# steps that evaluate expressions. Without that, the outer predicate reaches the source's key
# condition, granules are skipped by the values of the rows the view hides, and the `read_rows` of
# the query tells the invoker whether such a row exists — a one-bit oracle per query that needs no
# exception to read out.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user04758_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
-- \`key\` is the sort key, so it is what the outer predicate would prune on. Enough rows for
-- several granules, so that pruning is visible in \`read_rows\` at all.
CREATE TABLE $db.owned (key UInt64, owner String)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;
INSERT INTO $db.owned SELECT number, 'nobody' FROM numbers(100000);

-- Exposes nothing to the invoker: every row is owned by somebody else.
CREATE VIEW $db.owned_view
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.owned WHERE owner = currentUser();

CREATE TABLE $db.lazy_final_owned (key UInt64, version UInt64, owner String)
ENGINE = ReplacingMergeTree(version) ORDER BY key;
SYSTEM STOP MERGES $db.lazy_final_owned;
INSERT INTO $db.lazy_final_owned SELECT number, 1, 'nobody' FROM numbers(100000);
INSERT INTO $db.lazy_final_owned SELECT number, 2, 'nobody' FROM numbers(100000);
INSERT INTO $db.lazy_final_owned SELECT number + 100000, 1, 'nobody' FROM numbers(100000);

CREATE VIEW $db.lazy_final_owned_view
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.lazy_final_owned FINAL WHERE owner = currentUser();

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.owned_view TO $user;
GRANT SELECT ON $db.lazy_final_owned_view TO $user;
EOF

# `99999` exists in the table but is hidden by the view; `500000` exists nowhere. With the barrier
# the two must read the same number of rows, so the invoker learns nothing about the hidden row.
#
# The comparison is an exact equality of `read_rows` between two runs, so the per-query random
# read-path injections the test harness enables must be pinned off, and a single thread keeps the
# read pool deterministic — none of them affects the index analysis the test guards.
probe() {
    local query_id="probe_${CLICKHOUSE_DATABASE}_$1_$2"
    ${CLICKHOUSE_CLIENT} --enable_analyzer "$1" --user "$user" --query_id "$query_id" \
        --max_threads 1 \
        --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0 \
        --page_cache_inject_eviction 0 \
        --query "SELECT count() FROM $db.owned_view WHERE key = $2" > /dev/null
    echo "$query_id"
}

echo "===== the view exposes no row ====="
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM $db.owned_view"

echo "===== reading the view costs the same whether or not the hidden row matches ====="
for analyzer in 1 0; do
    hidden_id=$(probe "$analyzer" 99999)
    absent_id=$(probe "$analyzer" 500000)

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
done

echo "===== lazy FINAL does not consume an outer limit through the barrier ====="
# The third part has a non-overlapping key range. Lazy FINAL may split it out only when the outer
# LIMIT is not considered to apply to the read below the barrier. Before the fence this query kept
# regular FINAL instead, because the invoker's LIMIT disabled the partial split.
# The match is done in the shell rather than by wrapping the `EXPLAIN` into a subquery, because
# the latter needs `CREATE TEMPORARY TABLE ON *.*`, which the invoker deliberately does not have.
if ${CLICKHOUSE_CLIENT} --enable_analyzer 1 --user "$user" --query "
    EXPLAIN SELECT * FROM $db.lazy_final_owned_view LIMIT 1
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0" | grep -q "LazyFinal"
then echo 1; else echo 0; fi

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
