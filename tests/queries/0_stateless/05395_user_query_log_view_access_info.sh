#!/usr/bin/env bash
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=helpers/query_access_info.sh
. "$CUR_DIR"/helpers/query_access_info.sh

cleanup()
{
    qai_cleanup_users sealed invoker
}
trap cleanup EXIT

sealed_user=$(qai_user_identifier "qai_${CLICKHOUSE_TEST_UNIQUE_NAME}_sealed")

run_sql_input "$CUR_DIR"/05395_user_query_log_view_access_info.before.sql.in
expect_access_denied "EXECUTE AS ${sealed_user} SELECT count() FROM t_05395_private"
run_sql_input "$CUR_DIR"/05395_user_query_log_view_access_info.after.sql.in
