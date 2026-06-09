#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Access control for parameterized views in the analyzer.
# A parameterized view is resolved as a table function node, but it reads a real view storage, so it must be
# subject to the same SELECT/SHOW COLUMNS checks as a regular view (otherwise the query could read the view
# without any grant).

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${user};
CREATE USER ${user} NOT IDENTIFIED;

DROP TABLE IF EXISTS base;
CREATE TABLE base (x UInt32, s String) ENGINE = MergeTree ORDER BY x;
INSERT INTO base VALUES (1, 'a'), (2, 'b'), (3, 'c');

DROP VIEW IF EXISTS pv_invoker;
CREATE VIEW pv_invoker SQL SECURITY INVOKER AS SELECT x, s FROM base WHERE x < {n:UInt32};

DROP VIEW IF EXISTS pv_definer;
CREATE VIEW pv_definer SQL SECURITY DEFINER AS SELECT x, s FROM base WHERE x < {n:UInt32};
"

echo "-- SELECT without any grant must be denied --"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT * FROM pv_invoker(n = 3)" 2>&1 | grep -o "ACCESS_DENIED" | head -n 1

echo "-- DESCRIBE without SHOW COLUMNS grant must be denied --"
${CLICKHOUSE_CLIENT} --user "${user}" --query "DESCRIBE pv_invoker(n = 3)" 2>&1 | grep -o "ACCESS_DENIED" | head -n 1

echo "-- SELECT after granting SELECT on the view --"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv_definer TO ${user}"
# Honoring `SQL SECURITY DEFINER` for a parameterized view (so a grant on the view alone suffices, without a
# grant on the inner `base` table) works only in the analyzer; the old interpreter still demands access to the
# inner table. Pin the analyzer here, while the access denial checks above are valid for both and stay unpinned.
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT * FROM pv_definer(n = 3) ORDER BY x FORMAT TSV SETTINGS allow_experimental_analyzer = 1"

echo "-- DESCRIBE of a SQL SECURITY DEFINER view needs only SHOW COLUMNS, not grants on inner tables --"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.pv_definer FROM ${user}"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.pv_definer TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "DESCRIBE pv_definer(n = 3)" | cut -f1,2

${CLICKHOUSE_CLIENT} --query "
DROP VIEW IF EXISTS pv_invoker;
DROP VIEW IF EXISTS pv_definer;
DROP TABLE IF EXISTS base;
DROP USER IF EXISTS ${user};
"
