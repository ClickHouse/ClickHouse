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

echo "--- once ALTER ADD PROJECTION is revoked a stored definition cannot probe the table ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_probe;
    CREATE TABLE t_probe (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_probe SELECT number, number FROM numbers(100);
    GRANT ALTER ADD PROJECTION, SELECT ON ${CLICKHOUSE_DATABASE}.t_probe TO ${user};
"
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "CREATE HYPOTHETICAL PROJECTION p_probe ON ${CLICKHOUSE_DATABASE}.t_probe (SELECT a, b ORDER BY b)"
$CLICKHOUSE_CLIENT -q "REVOKE ALTER ADD PROJECTION ON ${CLICKHOUSE_DATABASE}.t_probe FROM ${user}; ALTER TABLE t_probe DROP COLUMN b SETTINGS mutations_sync = 2;"
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "EXPLAIN WHATIF SELECT a FROM ${CLICKHOUSE_DATABASE}.t_probe WHERE a = 1 SETTINGS optimize_use_projections = 1" \
    | grep -oE 'ACCESS_DENIED|no longer be added' | head -1

echo "--- without SELECT on a projection column the estimate is denied before any drift is told ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_cols; DROP TABLE IF EXISTS t_cols_empty;
    CREATE TABLE t_cols (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_cols SELECT number, number, number FROM numbers(100);
    CREATE TABLE t_cols_empty AS t_cols;
    GRANT ALTER ADD PROJECTION ON ${CLICKHOUSE_DATABASE}.t_cols TO ${user};
    GRANT ALTER ADD PROJECTION ON ${CLICKHOUSE_DATABASE}.t_cols_empty TO ${user};
    GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.t_cols TO ${user};
    GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.t_cols_empty TO ${user};
"
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "CREATE HYPOTHETICAL PROJECTION p_cols ON ${CLICKHOUSE_DATABASE}.t_cols (SELECT a, b, v ORDER BY b)"
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "CREATE HYPOTHETICAL PROJECTION p_cols ON ${CLICKHOUSE_DATABASE}.t_cols_empty (SELECT a, b, v ORDER BY b)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_cols DROP COLUMN b SETTINGS mutations_sync = 2; ALTER TABLE t_cols_empty DROP COLUMN b SETTINGS mutations_sync = 2;"
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "EXPLAIN WHATIF SELECT a FROM ${CLICKHOUSE_DATABASE}.t_cols WHERE a = 1 SETTINGS optimize_use_projections = 1" \
    | grep -oE 'ACCESS_DENIED|no longer be added' | head -1
${CLICKHOUSE_CURL} -sS "${url}" --data-binary "EXPLAIN WHATIF SELECT a FROM ${CLICKHOUSE_DATABASE}.t_cols_empty WHERE a = 1 SETTINGS optimize_use_projections = 1" \
    | grep -oE 'ACCESS_DENIED|no longer be added|Table is empty' | head -1

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}; DROP TABLE IF EXISTS t_vis_hidden; DROP TABLE IF EXISTS t_probe; DROP TABLE IF EXISTS t_cols; DROP TABLE IF EXISTS t_cols_empty;"
