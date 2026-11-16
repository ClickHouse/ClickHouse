#!/usr/bin/env bash
# a session entry outlives the grants it was made under, so the system tables must re-check visibility

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05054_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_vis;
    CREATE TABLE t_vis (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} NOT IDENTIFIED;
    GRANT ALTER ADD PROJECTION, SELECT, SHOW TABLES ON ${CLICKHOUSE_DATABASE}.t_vis TO ${user};
    GRANT SELECT ON system.hypothetical_indexes TO ${user};
    GRANT SELECT ON system.hypothetical_projections TO ${user};
"

# the store is per session, so the rename has to land between two statements of one HTTP session
url="${CLICKHOUSE_URL}&user=${user}&session_id=${CLICKHOUSE_DATABASE}_vis&session_timeout=600"
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "CREATE HYPOTHETICAL INDEX i_vis ON ${CLICKHOUSE_DATABASE}.t_vis (b) TYPE minmax GRANULARITY 1"
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "CREATE HYPOTHETICAL PROJECTION p_vis ON ${CLICKHOUSE_DATABASE}.t_vis (SELECT a, b ORDER BY b)"

echo "--- while the table is visible ---"
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "SELECT 'indexes:', count() FROM system.hypothetical_indexes WHERE name = 'i_vis'"
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "SELECT 'projections:', count() FROM system.hypothetical_projections WHERE name = 'p_vis'"

echo "--- after the table is renamed out of reach ---"
$CLICKHOUSE_CLIENT -q "RENAME TABLE t_vis TO t_vis_hidden;"
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "SELECT 'indexes:', count() FROM system.hypothetical_indexes WHERE name = 'i_vis'"
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "SELECT 'projections:', count() FROM system.hypothetical_projections WHERE name = 'p_vis'"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}; DROP TABLE IF EXISTS t_vis_hidden;"
