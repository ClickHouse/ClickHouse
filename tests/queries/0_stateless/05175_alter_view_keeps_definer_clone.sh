#!/usr/bin/env bash
# Tags: no-replicated-database, no-async-insert

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
# One ephemeral definer per object: the collection branch is reached only when the altered object
# is the last holder of its definer.
mv_user="mv_definer_${CLICKHOUSE_DATABASE}_$RANDOM"
view_user="view_definer_${CLICKHOUSE_DATABASE}_$RANDOM"
next_user="next_definer_${CLICKHOUSE_DATABASE}_$RANDOM"
mv_clone="${mv_user}:definer"
view_clone="${view_user}:definer"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $mv_user, $view_user, $next_user;
CREATE USER $mv_user IN memory;
CREATE USER $view_user IN memory;
CREATE USER $next_user;
GRANT SELECT, INSERT ON $db.* TO $mv_user;
GRANT SELECT ON $db.* TO $view_user;
GRANT SELECT, INSERT ON $db.* TO $next_user;

CREATE TABLE $db.source (x Int64) ENGINE = MergeTree() ORDER BY x;

CREATE MATERIALIZED VIEW $db.mv
(
    x Int64
)
ENGINE = MergeTree() ORDER BY x
DEFINER = $mv_user SQL SECURITY DEFINER
AS SELECT x FROM $db.source;

CREATE VIEW $db.v
DEFINER = $view_user SQL SECURITY DEFINER
AS SELECT x FROM $db.source;

DROP USER $mv_user, $view_user;
EOF

clone_count() { ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.users WHERE name = '$1'"; }

echo "mv clone after DROP USER: $(clone_count "$mv_clone")"
echo "view clone after DROP USER: $(clone_count "$view_clone")"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE $db.mv MODIFY COMMENT 'altered'"
echo "mv clone after MODIFY COMMENT: $(clone_count "$mv_clone")"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE $db.v MODIFY COMMENT 'altered'"
echo "view clone after MODIFY COMMENT: $(clone_count "$view_clone")"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE $db.mv MODIFY SQL SECURITY DEFINER DEFINER = '$mv_clone'"
echo "mv clone after MODIFY SQL SECURITY with the same definer: $(clone_count "$mv_clone")"

${CLICKHOUSE_CLIENT} --query "INSERT INTO $db.source VALUES (40)"
echo "mv sum: $(${CLICKHOUSE_CLIENT} --query "SELECT sum(x) FROM $db.mv")"
echo "view rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $db.v")"

# Changing the definer must still release the previous one and register the new one.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $db.mv MODIFY SQL SECURITY DEFINER DEFINER = $next_user"
echo "mv clone after the definer changed: $(clone_count "$mv_clone")"
${CLICKHOUSE_CLIENT} --query "DROP USER $next_user" 2>&1 | grep -q "HAVE_DEPENDENT_OBJECTS" && echo "new definer is protected"

# Dropping the object still collects its clone.
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.v"
echo "view clone after DROP TABLE: $(clone_count "$view_clone")"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.mv"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.source"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $next_user"
