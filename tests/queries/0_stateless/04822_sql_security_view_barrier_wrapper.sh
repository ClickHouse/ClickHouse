#!/usr/bin/env bash
# A `SQL SECURITY DEFINER` view whose row-hiding lives below a wrapper source — a `Merge` table, a
# remote table, a table function, or a nested view — is still an optimization barrier. `canHideRows`
# cannot prove such a view row-preserving, so the plan node that converts the view's result to the
# view's structure is sealed as a barrier even when the local walk over the plan finds no
# row-dropping step (the wrapper carries its hiding in its own child plans, not in `node->children`).
# Without the seal, later pushdown passes could move an invoker-controlled expression into the
# wrapper source and evaluate it on the rows the view hides.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user04822_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.secret (key UInt64, owner String, val String)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;
INSERT INTO $db.secret SELECT number, 'nobody', concat('secret-', toString(number)) FROM numbers(100000);

-- The nested view does the row hiding, and it sits below the \`Merge\` wrapper the outer view reads.
CREATE VIEW $db.inner_v
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.secret WHERE owner = currentUser();

CREATE TABLE $db.wrapper (key UInt64, owner String, val String)
ENGINE = Merge('$db', '^inner_v\$');

-- The outer view has no filter of its own: whatever it hides is hidden by the wrapper below it.
CREATE VIEW $db.v
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.wrapper;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.v TO $user;
EOF

echo "===== the view exposes no row ====="
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM $db.v"

echo "===== an exception oracle over a hidden value cannot fire ====="
for analyzer in 1 0; do
    ${CLICKHOUSE_CLIENT} --enable_analyzer "$analyzer" --user "$user" \
        --query "SELECT count() FROM $db.v WHERE throwIf(val = 'secret-99999', 'DISCLOSED')" 2>&1 \
        | grep -o 'DISCLOSED' || echo "not disclosed"
done

echo "===== reading the view costs the same whether or not the hidden row matches ====="
probe() {
    local query_id="probe_${CLICKHOUSE_DATABASE}_$1_$2"
    ${CLICKHOUSE_CLIENT} --enable_analyzer "$1" --user "$user" --query_id "$query_id" \
        --max_threads 1 \
        --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0 \
        --page_cache_inject_eviction 0 \
        --query "SELECT count() FROM $db.v WHERE key = $2" > /dev/null
    echo "$query_id"
}

for analyzer in 1 0; do
    hidden_id=$(probe "$analyzer" 99999)
    absent_id=$(probe "$analyzer" 500000)

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT multiIf(
            count() != 2, 'MISSING',
            anyIf(read_rows, query_id = '$hidden_id') = anyIf(read_rows, query_id = '$absent_id'),
            'same', 'DISCLOSED')
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND query_id IN ('$hidden_id', '$absent_id') AND type = 'QueryFinish'"
done

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
