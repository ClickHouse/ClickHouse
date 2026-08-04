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

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.owned_view TO $user;
EOF

# `99999` exists in the table but is hidden by the view; `500000` exists nowhere. With the barrier
# the two must read the same number of rows, so the invoker learns nothing about the hidden row.
probe() {
    local query_id="probe_${CLICKHOUSE_DATABASE}_$1_$2"
    ${CLICKHOUSE_CLIENT} --enable_analyzer "$1" --user "$user" --query_id "$query_id" --query \
        "SELECT count() FROM $db.owned_view WHERE key = $2" > /dev/null
    echo "$query_id"
}

echo "===== the view exposes no row ====="
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM $db.owned_view"

echo "===== reading the view costs the same whether or not the hidden row matches ====="
for analyzer in 1 0; do
    hidden_id=$(probe "$analyzer" 99999)
    absent_id=$(probe "$analyzer" 500000)

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT if(
            anyIf(read_rows, query_id = '$hidden_id') = anyIf(read_rows, query_id = '$absent_id'),
            'same', 'DISCLOSED')
        FROM system.query_log
        WHERE query_id IN ('$hidden_id', '$absent_id') AND type = 'QueryFinish'"
done

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
