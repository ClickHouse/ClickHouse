#!/usr/bin/env bash

# An `Alias` table forwards reads to its target table, and the real `SELECT` requires the `SELECT`
# grant on both the alias object and the target table (`StorageAlias::read` checks the target when
# the plan is built). `EXPLAIN QUERY TREE` / `EXPLAIN SYNTAX` under the analyzer never build a plan,
# so their access check has to reproduce the target-table check as well: a user with `SELECT` on the
# alias but not on its target must get the same denial from `EXPLAIN` as from the real `SELECT`,
# for an explicit column list and for the trivial-count fallback alike.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${user};
CREATE TABLE ${db}.target (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${db}.target VALUES (1, 'a');
CREATE TABLE ${db}.alias_t ENGINE = Alias('${db}', 'target');
CREATE USER ${user};
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

probe() {
    run "  SELECT column"              "SELECT y FROM ${db}.alias_t"
    run "  EXPLAIN QUERY TREE column"  "EXPLAIN QUERY TREE SELECT y FROM ${db}.alias_t"
    run "  EXPLAIN SYNTAX column"      "EXPLAIN SYNTAX SELECT y FROM ${db}.alias_t"
    run "  SELECT count()"             "SELECT count() FROM ${db}.alias_t"
    run "  EXPLAIN QUERY TREE count()" "EXPLAIN QUERY TREE SELECT count() FROM ${db}.alias_t"
    run "  EXPLAIN SYNTAX count()"     "EXPLAIN SYNTAX SELECT count() FROM ${db}.alias_t"
}

echo "-- SELECT on the alias only: EXPLAIN must be denied exactly as the real SELECT"
probe

${CLICKHOUSE_CLIENT} --query "GRANT SELECT(y) ON ${db}.target TO ${user}"

echo "-- Column grant on the target: the granted column and the trivial count become allowed"
probe
run "  SELECT ungranted column"             "SELECT z FROM ${db}.alias_t"
run "  EXPLAIN QUERY TREE ungranted column" "EXPLAIN QUERY TREE SELECT z FROM ${db}.alias_t"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db}.target TO ${user}"

echo "-- Full grant on the target: everything is allowed"
probe

${CLICKHOUSE_CLIENT} --query "
DROP TABLE ${db}.alias_t;
DROP TABLE ${db}.target;
DROP USER ${user};
"
