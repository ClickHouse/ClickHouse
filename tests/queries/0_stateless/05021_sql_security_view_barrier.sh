#!/usr/bin/env bash
# Tags: no-parallel-replicas

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user05021_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.secrets VALUES ('${user}', 'visible'), ('someone_else', 'HIDDEN');

-- Restricts which rows the invoker may see. Must be a security boundary.
CREATE VIEW $db.filtering_view
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.secrets WHERE owner = currentUser();

-- Hides no rows, so it must stay fully optimizable.
CREATE VIEW $db.projecting_view
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.secrets;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.filtering_view TO $user;
GRANT SELECT ON $db.projecting_view TO $user;
EOF

echo "===== the view filters ====="
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM $db.filtering_view"

echo "===== an outer predicate cannot observe the filtered-out rows ====="
# Without the barrier the outer WHERE is merged with the view's WHERE into a single filter over
# the source table, and throwIf fires on a row the view is supposed to hide.
for analyzer in 1 0; do
    ${CLICKHOUSE_CLIENT} --enable_analyzer "$analyzer" --user "$user" --query \
        "SELECT * FROM $db.filtering_view WHERE throwIf(secret = 'HIDDEN', 'LEAKED')" 2>&1 |
        grep -c -F "LEAKED"
done

echo "===== the same through a leaky cast ====="
# The exception message of a failing cast carries the value itself, which turns the oracle above
# into a plain read of the hidden row.
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT * FROM $db.filtering_view WHERE toUInt8(secret) = 1" 2>&1 | grep -c -F "HIDDEN"

echo "===== a view that hides no rows is not a barrier ====="
# PREWHERE for the outer predicate must survive, otherwise every DEFINER view pays for the fix.
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "EXPLAIN actions = 0, indexes = 0 SELECT * FROM $db.projecting_view WHERE secret = 'x'" |
    grep -c -F "ReadFromMergeTree"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT trimLeft(explain) FROM (
         EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.projecting_view WHERE secret = 'x'
     ) WHERE explain ILIKE '%Prewhere filter%' LIMIT 1"

echo "===== SQL SECURITY INVOKER is unaffected ====="
${CLICKHOUSE_CLIENT} <<EOF
CREATE VIEW $db.invoker_view
SQL SECURITY INVOKER
AS SELECT * FROM $db.secrets WHERE owner = currentUser();
GRANT SELECT ON $db.invoker_view TO $user;
GRANT SELECT ON $db.secrets TO $user;
EOF
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "EXPLAIN actions = 0, indexes = 0 SELECT * FROM $db.invoker_view WHERE secret = 'x'" |
    grep -c -F "ReadFromMergeTree"

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
