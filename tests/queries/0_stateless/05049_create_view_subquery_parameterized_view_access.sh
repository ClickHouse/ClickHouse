#!/usr/bin/env bash

# A parameterized view is resolved as a `TableFunctionNode`, which `extractAllTableReferences` used
# to drop, so the analyze-only subquery access check (`SelectQueryOptions::check_subquery_table_access`,
# used by `InterpreterCreateQuery::getSampleBlock` under the analyzer) never verified the `SELECT`
# grant on a parameterized view sitting inside a `FROM` subquery. A user without that grant could
# `CREATE VIEW ... AS SELECT * FROM (SELECT * FROM pv(...))` and learn the view's column names and
# types from the created object's structure, while the same statement with `pv(...)` as the top-level
# table expression was already denied. Both shapes must be denied the same way.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${user};
CREATE TABLE ${db}.base (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${db}.base VALUES (1, 'a');
CREATE VIEW ${db}.pv AS SELECT y, z FROM ${db}.base WHERE y = {n:Int32};
CREATE TABLE ${db}.plain (y Int32) ENGINE = MergeTree ORDER BY y;
CREATE USER ${user};
GRANT CREATE VIEW ON ${db}.* TO ${user};
GRANT SELECT ON ${db}.plain TO ${user};
GRANT SELECT ON ${db}.base TO ${user};
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

echo "-- No SELECT on the parameterized view: CREATE VIEW over it must be denied in every shape"
run "pv in a FROM subquery"        "CREATE VIEW ${db}.leak1 AS SELECT * FROM (SELECT * FROM ${db}.pv(n = 1))"
run "pv as the table expression"   "CREATE VIEW ${db}.leak2 AS SELECT * FROM ${db}.pv(n = 1)"
run "pv in a nested subquery"      "CREATE VIEW ${db}.leak3 AS SELECT * FROM (SELECT * FROM (SELECT * FROM ${db}.pv(n = 1)))"
run "pv joined inside a subquery"  "CREATE VIEW ${db}.leak4 AS SELECT * FROM (SELECT v.y FROM ${db}.plain AS p JOIN ${db}.pv(n = 1) AS v ON p.y = v.y)"

echo "-- Over-denial control: the same statements without the view stay allowed"
run "granted table in a subquery"  "CREATE VIEW ${db}.ok1 AS SELECT * FROM (SELECT * FROM ${db}.plain)"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db}.pv TO ${user}"

echo "-- With SELECT on the parameterized view everything is allowed"
run "pv in a FROM subquery"        "CREATE VIEW ${db}.ok2 AS SELECT * FROM (SELECT * FROM ${db}.pv(n = 1))"
run "pv as the table expression"   "CREATE VIEW ${db}.ok3 AS SELECT * FROM ${db}.pv(n = 1)"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW IF EXISTS ${db}.leak1, ${db}.leak2, ${db}.leak3, ${db}.leak4, ${db}.ok1, ${db}.ok2, ${db}.ok3;
DROP VIEW ${db}.pv;
DROP TABLE ${db}.base;
DROP TABLE ${db}.plain;
DROP USER ${user};
"
