#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Verifies that DDL-revealing columns in `system.tables` require `SHOW_COLUMNS`,
# preventing column-only grantees from reading the full DDL of a table that
# `DESCRIBE TABLE` / `SHOW CREATE TABLE` correctly refuses them.

user="user04257_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE USER $user;
DROP TABLE IF EXISTS $db.secret;
CREATE TABLE $db.secret (x UInt32, public_col UInt32, secret String)
ENGINE = MergeTree
PARTITION BY x
ORDER BY (x, public_col)
SAMPLE BY x;
DROP TABLE IF EXISTS $db.dep_view;
CREATE MATERIALIZED VIEW $db.dep_view ENGINE = MergeTree ORDER BY x AS SELECT x FROM $db.secret;
EOF

run_user() {
    ${CLICKHOUSE_CLIENT} --user "$user" --query "$1"
}

# Probes: each emits a single line of 0/1 flags describing what is visible.
ddl_probe="
SELECT
    length(create_table_query) > 0,
    length(engine_full) > 0,
    length(partition_key) > 0,
    length(sorting_key) > 0,
    length(primary_key) > 0,
    length(sampling_key) > 0
FROM system.tables WHERE database = '$db' AND name = 'secret'"

deps_probe="
SELECT length(dependencies_database), length(dependencies_table)
FROM system.tables WHERE database = '$db' AND name = 'secret'"

name_probe="SELECT name FROM system.tables WHERE database = '$db' AND name = 'secret'"

echo "--- column-only grant ---"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(public_col) ON $db.secret TO $user"

# Row visible (SHOW_TABLES implicit from column grant).
run_user "$name_probe"

# DDL columns hidden — all flags must be 0.
run_user "$ddl_probe"

# Dependent view hidden — user has no SHOW_TABLES on $db.dep_view.
run_user "$deps_probe"

echo "--- + SHOW COLUMNS grant ---"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON $db.secret TO $user"

# DDL now visible — all flags must be 1.
run_user "$ddl_probe"

echo "--- table-level SELECT grant (regression guard) ---"
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW COLUMNS ON $db.secret FROM $user"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(public_col) ON $db.secret FROM $user"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.secret TO $user"

# Table-level SELECT implies SHOW_COLUMNS via column-flag rule — DDL visible.
run_user "$ddl_probe"

echo "--- SHOW TABLES only grant ---"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON $db.secret FROM $user"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW TABLES ON $db.secret TO $user"

# Row visible.
run_user "$name_probe"

# DDL hidden — `SHOW_TABLES` is not `SHOW_COLUMNS`.
run_user "$ddl_probe"

echo "--- dependent view visibility ---"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW TABLES ON $db.dep_view TO $user"

# Dependent view now exposed.
run_user "$deps_probe"

echo "--- db-level SHOW TABLES grant ---"
# A user with `GRANT SHOW TABLES ON $db.*` (no `SHOW COLUMNS`) must still see empty
# DDL columns — db-level `SHOW TABLES` is not `SHOW COLUMNS`.
${CLICKHOUSE_CLIENT} --query "DROP USER $user"
${CLICKHOUSE_CLIENT} --query "CREATE USER $user"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW TABLES ON $db.* TO $user"

run_user "$name_probe"
run_user "$ddl_probe"

echo "--- cross-db dependent view filter ---"
# Dependent view in another database must be filtered when the user has no rights
# on that database, even when they have db-level `SHOW TABLES` on the source.
db2="${db}_d"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS $db2"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE $db2"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW $db2.cross_dep ENGINE = MergeTree ORDER BY x AS SELECT x FROM $db.secret"

# User has SHOW TABLES on $db.* (which covers dep_view) but not on $db2.
# Expected: only dep_view shown, cross_dep filtered out.
${CLICKHOUSE_CLIENT} --user "$user" --query "
SELECT
    length(dependencies_table),
    arrayExists(x -> x = 'dep_view', dependencies_table),
    arrayExists(x -> x = 'cross_dep', dependencies_table)
FROM system.tables WHERE database = '$db' AND name = 'secret'"

${CLICKHOUSE_CLIENT} <<EOF
DROP DATABASE IF EXISTS $db2;
DROP USER IF EXISTS $user;
DROP TABLE IF EXISTS $db.dep_view;
DROP TABLE IF EXISTS $db.secret;
EOF
