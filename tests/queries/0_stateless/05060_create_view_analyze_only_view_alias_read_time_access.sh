#!/usr/bin/env bash

# `CREATE ... AS SELECT` infers the structure of the created object by analyzing the `SELECT` without
# planning or reading anything (`only_analyze`), so the checks the storages perform only when a plan is
# built never ran for it: `StorageView::readImpl` checks `SELECT` on the base tables of a
# `SQL SECURITY INVOKER` view and `StorageAlias::read` checks the target table of an `Alias`. A user with
# `SELECT` on the view or alias object but not on the underlying table could thus
# `CREATE VIEW dst AS SELECT * FROM v_invoker` (or `... FROM (SELECT * FROM v_invoker)`) and learn the
# column names and types of a query whose real `SELECT` is denied. Every shape must be denied exactly
# when the real `SELECT` is, and stay allowed once the underlying grant exists - with column-level grants
# honoured the same way the real `SELECT` honours them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${user};
CREATE TABLE ${db}.base (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${db}.base VALUES (1, 'a');
CREATE VIEW ${db}.v_invoker SQL SECURITY INVOKER AS SELECT y, z FROM ${db}.base;
CREATE VIEW ${db}.v_definer SQL SECURITY DEFINER DEFINER = CURRENT_USER AS SELECT y, z FROM ${db}.base;
CREATE TABLE ${db}.alias_t ENGINE = Alias('${db}', 'base');
CREATE USER ${user};
GRANT CREATE VIEW ON ${db}.* TO ${user};
GRANT SELECT ON ${db}.v_invoker TO ${user};
GRANT SELECT ON ${db}.v_definer TO ${user};
GRANT SELECT ON ${db}.alias_t TO ${user};
"

# Print 'OK' only when the query succeeds, 'ACCESS_DENIED' when it is rejected for access reasons,
# and the full unexpected error otherwise (which makes the reference diff fail) so that a positive
# case that starts throwing a different exception cannot silently pass.
run() {
    local label="$1"
    local query="$2"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer 1 --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

# `CREATE VIEW` of a fresh object over the given `SELECT`: `CREATE OR REPLACE` would additionally require
# grants on the temporary replacement table, so every statement creates a new view instead.
n=0
cv() {
    local label="$1"
    local select="$2"
    n=$((n + 1))
    run "${label}" "CREATE VIEW ${db}.dst_${n} AS ${select}"
}

# Each shape as the real SELECT and as the CREATE VIEW that only analyzes it; both must agree.
probe() {
    local label="$1"
    local from="$2"
    run "  SELECT ${label}"                       "SELECT * FROM ${from}"
    cv "  CREATE VIEW ${label}"                  "SELECT * FROM ${from}"
    cv "  CREATE VIEW ${label} in a subquery"    "SELECT * FROM (SELECT * FROM ${from})"
    cv "  CREATE VIEW ${label} count()"          "SELECT count() FROM ${from}"
    cv "  CREATE VIEW ${label} count() in a subquery" "SELECT * FROM (SELECT count() FROM ${from})"
}

echo "-- SELECT on the view / alias object only: CREATE VIEW must be denied exactly as the real SELECT"
probe "INVOKER view" "${db}.v_invoker"
probe "Alias"        "${db}.alias_t"

echo "-- Over-denial control: a DEFINER view reads its base table as the definer, so it stays allowed"
probe "DEFINER view" "${db}.v_definer"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT(y) ON ${db}.base TO ${user}"

echo "-- Column grant on the base table: only the granted column is readable, in every shape"
run "  SELECT granted column through the view"                  "SELECT y FROM ${db}.v_invoker"
cv "  CREATE VIEW granted column through the view"             "SELECT y FROM ${db}.v_invoker"
cv "  CREATE VIEW granted column through the view in a subquery" "SELECT * FROM (SELECT y FROM ${db}.v_invoker)"
run "  SELECT ungranted column through the view"                "SELECT z FROM ${db}.v_invoker"
cv "  CREATE VIEW ungranted column through the view"           "SELECT z FROM ${db}.v_invoker"
cv "  CREATE VIEW ungranted column through the view in a subquery" "SELECT * FROM (SELECT z FROM ${db}.v_invoker)"
run "  SELECT granted column through the alias"                 "SELECT y FROM ${db}.alias_t"
cv "  CREATE VIEW granted column through the alias"            "SELECT y FROM ${db}.alias_t"
cv "  CREATE VIEW granted column through the alias in a subquery" "SELECT * FROM (SELECT y FROM ${db}.alias_t)"
run "  SELECT ungranted column through the alias"               "SELECT z FROM ${db}.alias_t"
cv "  CREATE VIEW ungranted column through the alias"          "SELECT z FROM ${db}.alias_t"
cv "  CREATE VIEW ungranted column through the alias in a subquery" "SELECT * FROM (SELECT z FROM ${db}.alias_t)"
cv "  CREATE VIEW count() through the alias"                   "SELECT count() FROM ${db}.alias_t"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db}.base TO ${user}"

echo "-- Full grant on the base table: everything is allowed"
probe "INVOKER view" "${db}.v_invoker"
probe "Alias"        "${db}.alias_t"

${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = '${db}' AND name LIKE 'dst_%' ORDER BY name" \
    | while read -r created; do ${CLICKHOUSE_CLIENT} --query "DROP VIEW ${db}.${created}" < /dev/null; done

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${db}.v_invoker;
DROP VIEW ${db}.v_definer;
DROP TABLE ${db}.alias_t;
DROP TABLE ${db}.base;
DROP USER ${user};
"
