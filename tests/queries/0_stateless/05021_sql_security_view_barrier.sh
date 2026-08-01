#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user05021_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.secrets VALUES ('${user}', 'visible'), ('someone_else', 'HIDDEN');

-- Restricts which rows the invoker may see, so it must be a security boundary.
CREATE VIEW $db.filtering_view
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT * FROM $db.secrets WHERE owner = currentUser();

CREATE VIEW $db.filtering_view_none
SQL SECURITY NONE
AS SELECT * FROM $db.secrets WHERE owner = currentUser();

-- Hides no rows, so it must stay fully optimizable.
CREATE VIEW $db.projecting_view
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT owner, secret FROM $db.secrets;

CREATE VIEW $db.invoker_view
SQL SECURITY INVOKER
AS SELECT * FROM $db.secrets WHERE owner = currentUser();

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.filtering_view TO $user;
GRANT SELECT ON $db.filtering_view_none TO $user;
GRANT SELECT ON $db.projecting_view TO $user;
GRANT SELECT ON $db.invoker_view TO $user;
GRANT SELECT ON $db.secrets TO $user;
EOF

echo "===== the view exposes one row ====="
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT secret FROM $db.filtering_view"

echo "===== an outer predicate cannot observe the filtered-out row ====="
# Without the barrier the outer WHERE and the view's WHERE are merged into one filter over the
# source table, and throwIf fires on a row the view is supposed to hide.
for view in filtering_view filtering_view_none; do
    for analyzer in 1 0; do
        ${CLICKHOUSE_CLIENT} --enable_analyzer "$analyzer" --user "$user" --query \
            "SELECT * FROM $db.$view WHERE throwIf(secret = 'HIDDEN', 'LEAKED')" 2>&1 |
            grep -c -F "LEAKED"
    done
done

echo "===== nor through the value in a cast error message ====="
# A failing cast puts the offending value in the exception, so the oracle above becomes a plain
# read of the hidden row. The cast still fails on the invoker's own row; only 'HIDDEN' must not
# appear.
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT * FROM $db.filtering_view WHERE toUInt8(secret) = 1" 2>&1 | grep -c -F "HIDDEN"

# The plan shape does not depend on who runs the query, and wrapping EXPLAIN in a subquery needs
# CREATE TEMPORARY TABLE, so these two run as the default user.
echo "===== a view that hides no rows is not a barrier ====="
# PREWHERE for the outer predicate must survive, otherwise every DEFINER view pays for the fix.
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() > 0 FROM (
         EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.projecting_view WHERE secret = 'x'
         SETTINGS optimize_move_to_prewhere = 1
     ) WHERE explain ILIKE '%Prewhere filter column: %secret%'"

echo "===== SQL SECURITY INVOKER is not a barrier either ====="
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() > 0 FROM (
         EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.invoker_view WHERE secret = 'x'
         SETTINGS optimize_move_to_prewhere = 1
     ) WHERE explain ILIKE '%Prewhere filter column: %secret%'"

echo "===== but a filtering DEFINER view keeps the outer predicate out of PREWHERE ====="
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() > 0 FROM (
         EXPLAIN actions = 1, indexes = 0 SELECT * FROM $db.filtering_view WHERE secret = 'x'
         SETTINGS optimize_move_to_prewhere = 1
     ) WHERE explain ILIKE '%Prewhere filter column: %secret%'"

echo "===== results through a barrier view are still correct ====="
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT secret FROM $db.filtering_view WHERE secret LIKE 'vis%'"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT count() FROM $db.filtering_view WHERE secret = 'HIDDEN'"

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
