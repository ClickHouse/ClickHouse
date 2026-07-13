#!/usr/bin/env bash

# Tests that the SELECT access check added for EXPLAIN QUERY TREE / EXPLAIN SYNTAX
# (https://github.com/ClickHouse/ClickHouse/issues/78938) uses the per-scope context, so it does
# not over-deny SQL SECURITY DEFINER / NONE views whose body is inlined with analyzer_inline_views.
# A user with SELECT on the view but not on its base table must still be able to EXPLAIN it, exactly
# as a plain SELECT through the view is allowed. A user with no access at all is still denied.
#
# Note: this test only covers scenarios where EXPLAIN must match a plain SELECT. The separate,
# pre-existing question of whether analyzer_inline_views should preserve a view's own access grant
# (currently it does not) is out of scope here.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

definer="definer_${CLICKHOUSE_DATABASE}"
reader="reader_${CLICKHOUSE_DATABASE}"
outsider="outsider_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${definer}, ${reader}, ${outsider};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.base VALUES (1, 'a');

CREATE USER ${definer}, ${reader}, ${outsider};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base TO ${definer};

CREATE VIEW ${CLICKHOUSE_DATABASE}.v_definer
DEFINER = ${definer} SQL SECURITY DEFINER
AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base;

CREATE VIEW ${CLICKHOUSE_DATABASE}.v_none
SQL SECURITY NONE
AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base;

-- The reader can read the views but has no direct access to the base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_definer TO ${reader};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_none TO ${reader};
"

# Print 'OK' only when the query succeeds, 'ACCESS_DENIED' when it is rejected for access reasons,
# and the full unexpected error otherwise (which makes the reference diff fail) so that a positive
# case that starts throwing a different exception cannot silently pass.
run() {
    local user="$1"
    local inline="$2"
    local label="$3"
    local query="$4"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer 1 --analyzer_inline_views "${inline}" --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

echo "-- Reader has SELECT on the view but not on the base table: EXPLAIN matches SELECT and is allowed"
run "${reader}" 0 "SELECT via DEFINER view"                        "SELECT * FROM ${CLICKHOUSE_DATABASE}.v_definer"
run "${reader}" 1 "SELECT via DEFINER view (inline)"               "SELECT * FROM ${CLICKHOUSE_DATABASE}.v_definer"
run "${reader}" 0 "EXPLAIN QUERY TREE DEFINER view"                "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.v_definer"
run "${reader}" 1 "EXPLAIN QUERY TREE DEFINER view (inline)"       "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.v_definer"
run "${reader}" 1 "EXPLAIN SYNTAX DEFINER view (inline)"           "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.v_definer"
run "${reader}" 1 "EXPLAIN QUERY TREE DEFINER view count (inline)" "EXPLAIN QUERY TREE SELECT count() FROM ${CLICKHOUSE_DATABASE}.v_definer"
run "${reader}" 1 "EXPLAIN QUERY TREE NONE view (inline)"          "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.v_none"

echo "-- Outsider has no access: EXPLAIN is denied exactly as a plain SELECT would be"
run "${outsider}" 0 "EXPLAIN QUERY TREE DEFINER view"     "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.v_definer"
run "${outsider}" 0 "EXPLAIN SYNTAX DEFINER view"         "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.v_definer"
run "${outsider}" 1 "EXPLAIN QUERY TREE base table (inline)" "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.base"

# Drop the view first to release the definer dependency, then the users.
${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v_definer;
DROP VIEW ${CLICKHOUSE_DATABASE}.v_none;
DROP USER ${definer}, ${reader}, ${outsider};
"
