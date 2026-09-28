#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the HMAC function is not available in the fast test build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The plan of a sealed view (`SQL SECURITY DEFINER` or `NONE`) runs with the view's privileges and may hold
# values folded from data the invoker cannot read: a scalar subquery over a private table, a key derived on
# the other side of a JOIN. EXPLAIN shows the plan to the definer, to a user who could have created the same view
# (`SET DEFINER` on the definer or `ALLOW SQL SECURITY NONE` for a `NONE` view, together with the privileges to read
# whatever a view may read: `SELECT`, `dictGet`, `READ`, `CREATE TEMPORARY TABLE` and `NAMED COLLECTION` on
# everything) and to a user who may display secrets (the server setting `display_secrets_in_show_and_select`, the session setting
# `format_display_secrets_in_show_and_select` and the `displaySecretsInShowAndSelect` privilege together);
# everyone else sees the `ReadFromSealedView` step alone. The test server keeps the server setting off, so the
# secrets path is not covered here. The plans are not dumped into the reference: only the presence of the
# private value, of the inner steps and of the hidden-plan marker is counted.

db=${CLICKHOUSE_DATABASE}
definer="definer_${db}_$RANDOM"
reader="reader_${db}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
DROP USER IF EXISTS $definer, $reader;
CREATE USER $definer;
CREATE USER $reader;

CREATE TABLE $db.private_table (s String) ENGINE = Memory;
INSERT INTO $db.private_table VALUES ('PRIVATE_ROW_VALUE');
GRANT SELECT ON $db.private_table TO $definer;
GRANT CREATE TEMPORARY TABLE ON *.* TO $definer;

CREATE VIEW $db.definer_view DEFINER = $definer SQL SECURITY DEFINER AS
    SELECT n.number FROM numbers(1) AS n
    INNER JOIN (SELECT 'JOIN_SIDE_KEY' AS k) AS s ON HMAC('sha256', toString(n.number), s.k) = ''
    WHERE toString(n.number) = (SELECT s FROM $db.private_table)
    SETTINGS enable_analyzer = 1;

CREATE VIEW $db.none_view SQL SECURITY NONE AS
    SELECT number FROM numbers(1) WHERE toString(number) = (SELECT s FROM $db.private_table)
    SETTINGS enable_analyzer = 1;

GRANT SELECT ON $db.definer_view TO $definer, $reader;
GRANT SELECT ON $db.none_view TO $definer, $reader;
EOSQL

# Prints, per EXPLAIN flavour: lines with the private value, lines with an inner step, lines with the marker.
explain_counts() {
    local user=$1 settings=$2 query=$3
    local user_option=()
    [ -n "$user" ] && user_option=(--user "$user")
    local out
    out=$(${CLICKHOUSE_CLIENT} "${user_option[@]}" ${settings} --query "$query" 2>&1)
    echo "$(grep -c 'PRIVATE_ROW_VALUE\|JOIN_SIDE_KEY' <<< "$out") $(grep -c 'ReadFromSystemNumbers' <<< "$out") $(grep -c 'plan hidden' <<< "$out")"
}

flavours=(
    "EXPLAIN PLAN actions = 1"
    "EXPLAIN PLAN actions = 1, pretty = 0"
    "EXPLAIN PLAN header = 1, indexes = 1"
    "EXPLAIN PLAN json = 1, actions = 1, header = 1"
    "EXPLAIN PIPELINE header = 1"
    "EXPLAIN PIPELINE graph = 1, header = 1"
    "EXPLAIN PIPELINE graph = 1, compact = 0"
    "EXPLAIN ESTIMATE"
)

for view in definer_view none_view; do
    echo "-- $view for the reader: private value, inner steps, hidden marker"
    for flavour in "${flavours[@]}"; do
        echo "$flavour: $(explain_counts "$reader" "" "$flavour SELECT * FROM $db.$view")"
    done

    echo "-- $view for the reader with the format setting but without the privilege"
    echo "$(explain_counts "$reader" "--format_display_secrets_in_show_and_select 1" "EXPLAIN PLAN actions = 1 SELECT * FROM $db.$view")"

    echo "-- $view: the reader still queries it"
    ${CLICKHOUSE_CLIENT} --user "$reader" --query "SELECT count() FROM $db.$view"
done

echo "-- the default user holds every privilege, so it sees both plans"
echo "$(explain_counts "" "" "EXPLAIN PLAN actions = 1 SELECT * FROM $db.definer_view")"
echo "$(explain_counts "" "" "EXPLAIN PLAN actions = 1 SELECT * FROM $db.none_view")"

echo "-- definer_view for its definer: the plan is shown, with the value folded from the private table"
echo "$(explain_counts "$definer" "" "EXPLAIN PLAN actions = 1 SELECT * FROM $db.definer_view")"
echo "$(explain_counts "$definer" "" "EXPLAIN PIPELINE header = 1 SELECT * FROM $db.definer_view")"
echo "-- none_view for the same user: no definer to match"
echo "$(explain_counts "$definer" "" "EXPLAIN PLAN actions = 1 SELECT * FROM $db.none_view")"

echo "-- a user with SET DEFINER on the definer but without SELECT on the private table does not see the plan"
${CLICKHOUSE_CLIENT} --query "GRANT SET DEFINER ON $definer TO $reader"
echo "$(explain_counts "$reader" "" "EXPLAIN PLAN actions = 1 SELECT * FROM $db.definer_view")"
echo "$(explain_counts "$reader" "" "EXPLAIN PLAN actions = 1 SELECT * FROM $db.none_view")"

echo "-- nor does a user with ALLOW SQL SECURITY NONE"
${CLICKHOUSE_CLIENT} --query "GRANT ALLOW SQL SECURITY NONE ON *.* TO $reader"
echo "$(explain_counts "$reader" "" "EXPLAIN PLAN actions = 1 SELECT * FROM $db.none_view")"

echo "-- with the privileges to read everything, the same user sees both plans"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, dictGet, READ, CREATE TEMPORARY TABLE, NAMED COLLECTION ON *.* TO $reader"
echo "$(explain_counts "$reader" "" "EXPLAIN PLAN actions = 1 SELECT * FROM $db.definer_view")"
echo "$(explain_counts "$reader" "" "EXPLAIN PLAN actions = 1 SELECT * FROM $db.none_view")"

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.definer_view; DROP VIEW $db.none_view; DROP TABLE $db.private_table; DROP USER $definer, $reader"
