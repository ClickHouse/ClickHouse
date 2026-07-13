#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user04401_${CLICKHOUSE_DATABASE}"
# Use an explicit Atomic database so the _tmp_replace_* rename branch is exercised on every CI
# shard. On the ordinary-database shard InterpreterCreateQuery routes CREATE OR REPLACE VIEW
# through doCreateTable (drop+recreate) instead of doCreateOrReplaceTable: the rename branch this
# test guards is reached only for Atomic/Replicated engines (InterpreterCreateQuery::createTable,
# replace_view && engine in {Atomic, Replicated}).
db="${CLICKHOUSE_DATABASE}_db"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS $db"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE $db ENGINE = Atomic"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "CREATE USER $user"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE $db.src (x Int64) ENGINE = MergeTree() ORDER BY x"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $db.mv_target (x Int64) ENGINE = MergeTree() ORDER BY x"

# CREATE OR REPLACE VIEW / MATERIALIZED VIEW builds the object under an internal _tmp_replace_*
# name, registers the definer dependency there, then renames it to the target. The definer
# dependency must move with the rename, not linger on the stale _tmp_replace_* name.

# Regular view: StorageView::renameInMemory.
${CLICKHOUSE_CLIENT} --query "CREATE OR REPLACE VIEW $db.v DEFINER = $user SQL SECURITY DEFINER AS SELECT x FROM $db.src"
# DROP USER must be rejected while the definer view exists (dependency followed the rename to the
# live view, not the stale temp name).
${CLICKHOUSE_CLIENT} --query "DROP USER $user" 2>&1 | grep -q "HAVE_DEPENDENT_OBJECTS" && echo "v rejected"
# Dropping the view frees the user.
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.v"
${CLICKHOUSE_CLIENT} --query "DROP USER $user" && echo "v freed"

# Materialized view: StorageMaterializedView::renameInMemory (extra target-table rename behavior).
${CLICKHOUSE_CLIENT} --query "CREATE USER $user"
${CLICKHOUSE_CLIENT} --query "CREATE OR REPLACE MATERIALIZED VIEW $db.mv TO $db.mv_target DEFINER = $user SQL SECURITY DEFINER AS SELECT x FROM $db.src"
${CLICKHOUSE_CLIENT} --query "DROP USER $user" 2>&1 | grep -q "HAVE_DEPENDENT_OBJECTS" && echo "mv rejected"
${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.mv"
${CLICKHOUSE_CLIENT} --query "DROP USER $user" && echo "mv freed"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE $db"
