#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the short ATTACH VIEW is rejected in a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
definer="definer04926_${db}_$RANDOM"
weak="weak04926_${db}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.src (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.src VALUES (42);

DROP USER IF EXISTS $definer;
CREATE USER $definer IDENTIFIED WITH no_password;
GRANT SELECT ON $db.src TO $definer;

CREATE VIEW $db.v_definer DEFINER = $definer SQL SECURITY DEFINER AS SELECT k FROM $db.src;
CREATE VIEW $db.v_none SQL SECURITY NONE AS SELECT k FROM $db.src;

DROP USER IF EXISTS $weak;
CREATE USER $weak IDENTIFIED WITH no_password;
GRANT SELECT, CREATE VIEW ON $db.* TO $weak;

DETACH VIEW $db.v_definer;
DETACH VIEW $db.v_none;
EOF

# The stored clauses require SET DEFINER / ALLOW SQL SECURITY NONE, which $weak does not hold.
${CLICKHOUSE_CLIENT} --user "$weak" --query "ATTACH VIEW $db.v_definer" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "NO ERROR"
${CLICKHOUSE_CLIENT} --user "$weak" --query "ATTACH VIEW $db.v_none" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "NO ERROR"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '$db' AND name IN ('v_definer', 'v_none')"

# An INVOKER view carries neither, so $weak may re-attach it and read it with its own rights.
${CLICKHOUSE_CLIENT} <<EOF
CREATE VIEW $db.v_invoker SQL SECURITY INVOKER AS SELECT k FROM $db.src;
GRANT SELECT ON $db.src TO $weak;
DETACH VIEW $db.v_invoker;
EOF
${CLICKHOUSE_CLIENT} --user "$weak" --query "ATTACH VIEW $db.v_invoker"
${CLICKHOUSE_CLIENT} --user "$weak" --query "SELECT k FROM $db.v_invoker"

# A user holding the required grants re-attaches both, and the stored clauses survive.
${CLICKHOUSE_CLIENT} <<EOF
ATTACH VIEW $db.v_definer;
ATTACH VIEW $db.v_none;
SELECT name, definer = '$definer' FROM system.tables WHERE database = '$db' AND name IN ('v_definer', 'v_none') ORDER BY name;
SELECT k FROM $db.v_definer;
EOF

# Both are attached now, so IF NOT EXISTS applies no definition and must not demand its grants.
${CLICKHOUSE_CLIENT} --user "$weak" --query "ATTACH VIEW IF NOT EXISTS $db.v_definer" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "NO ERROR"
${CLICKHOUSE_CLIENT} --user "$weak" --query "ATTACH VIEW IF NOT EXISTS $db.v_none" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "NO ERROR"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '$db' AND name IN ('v_definer', 'v_none')"

${CLICKHOUSE_CLIENT} <<EOF
DROP VIEW $db.v_definer;
DROP VIEW $db.v_none;
DROP VIEW $db.v_invoker;
DROP TABLE $db.src;
DROP USER $weak;
DROP USER $definer;
EOF
