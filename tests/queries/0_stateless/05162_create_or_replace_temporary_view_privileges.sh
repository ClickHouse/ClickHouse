#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE USER replace_temp_view_user; GRANT CREATE VIEW ON *.* TO replace_temp_view_user;"

# CREATE VIEW implicitly grants CREATE TEMPORARY_VIEW globally, and that is all that
# `CREATE OR REPLACE TEMPORARY VIEW` requires: the view lives in the session namespace,
# so no DROP_VIEW / CREATE_VIEW privilege on a database object is needed.
$CLICKHOUSE_CLIENT --user replace_temp_view_user -nm -q "
CREATE OR REPLACE TEMPORARY VIEW tview AS SELECT 1 AS x;
SELECT * FROM tview;
EXISTS TEMPORARY VIEW tview;
CREATE OR REPLACE TEMPORARY VIEW tview AS SELECT 2 AS x;
SELECT * FROM tview;
DROP TEMPORARY VIEW tview;
"

# A user without CREATE VIEW cannot create or replace a temporary view.
$CLICKHOUSE_CLIENT -q "CREATE USER no_temp_view_user;"

$CLICKHOUSE_CLIENT --user no_temp_view_user -nm -q "
CREATE OR REPLACE TEMPORARY VIEW tview AS SELECT 1; -- { serverError ACCESS_DENIED }
"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS replace_temp_view_user; DROP USER IF EXISTS no_temp_view_user;"
