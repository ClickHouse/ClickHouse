#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the short ATTACH VIEW is rejected in a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
weak="weak04926t_${db}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE $db.dst (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE $db.own (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.src VALUES (42);

CREATE MATERIALIZED VIEW $db.mv_dst TO $db.dst AS SELECT k FROM $db.src;
CREATE MATERIALIZED VIEW $db.mv_own TO $db.own AS SELECT k FROM $db.src;
CREATE MATERIALIZED VIEW $db.mv_inner ENGINE = MergeTree ORDER BY k AS SELECT k FROM $db.src;

DROP USER IF EXISTS $weak;
CREATE USER $weak IDENTIFIED WITH no_password;
GRANT CREATE VIEW ON $db.* TO $weak;
GRANT SELECT ON $db.src TO $weak;
GRANT SELECT, INSERT ON $db.own TO $weak;

DETACH VIEW $db.mv_dst;
DETACH VIEW $db.mv_own;
DETACH VIEW $db.mv_inner;
EOF

${CLICKHOUSE_CLIENT} --user "$weak" --query "ATTACH MATERIALIZED VIEW $db.mv_dst" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "NO ERROR"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '$db' AND name = 'mv_dst'"
${CLICKHOUSE_CLIENT} --user "$weak" --query "CREATE MATERIALIZED VIEW $db.mv_probe TO $db.dst AS SELECT k FROM $db.src" 2>&1 | grep -q "ACCESS_DENIED" && echo "ACCESS_DENIED" || echo "NO ERROR"

${CLICKHOUSE_CLIENT} --user "$weak" --query "ATTACH MATERIALIZED VIEW $db.mv_own"
${CLICKHOUSE_CLIENT} --user "$weak" --query "ATTACH MATERIALIZED VIEW $db.mv_inner"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = '$db' AND name IN ('mv_own', 'mv_inner') ORDER BY name"

${CLICKHOUSE_CLIENT} <<EOF
ATTACH MATERIALIZED VIEW $db.mv_dst;
SELECT extract(create_table_query, 'TO [^ ]+') = 'TO $db.dst' FROM system.tables WHERE database = '$db' AND name = 'mv_dst';
INSERT INTO $db.src VALUES (43);
SELECT count() FROM $db.dst;
DROP VIEW $db.mv_dst;
DROP VIEW $db.mv_own;
DROP VIEW $db.mv_inner;
DROP TABLE $db.src;
DROP TABLE $db.dst;
DROP TABLE $db.own;
DROP USER $weak;
EOF
