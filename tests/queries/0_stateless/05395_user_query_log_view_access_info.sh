#!/usr/bin/env bash
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

render_sql_input()
{
    python3 - "$1" "$CLICKHOUSE_TEST_UNIQUE_NAME" <<'PY_RENDER'
from pathlib import Path
import re
import sys

text = Path(sys.argv[1]).read_text()
unique_name = sys.argv[2]
for old_user in sorted(set(re.findall(r"user_[0-9]{5}_[A-Za-z0-9_]+", text)), key=len, reverse=True):
    suffix = old_user.split("_", 2)[2]
    user_name = f"qai_{unique_name}_{suffix}"
    quoted_identifier = "`" + user_name.replace("`", "``") + "`"
    quoted_literal = "'" + user_name.replace("\\", "\\\\").replace("'", "\\'") + "'"
    text = text.replace("'" + old_user + "'", quoted_literal)
    text = text.replace(old_user, quoted_identifier)
print(text, end="")
PY_RENDER
}

qai_user_identifier()
{
    python3 - "$1" <<'PY_QUOTE'
import sys
print("`" + sys.argv[1].replace("`", "``") + "`", end="")
PY_QUOTE
}

run_sql_input()
{
    render_sql_input "$1" | $CLICKHOUSE_CLIENT --multiquery
}

expect_access_denied()
{
    local output
    if output=$( $CLICKHOUSE_CLIENT --query "$1" 2>&1 ); then
        echo "Expected ACCESS_DENIED, but query succeeded" >&2
        return 1
    fi
    if [[ "$output" != *ACCESS_DENIED* && "$output" != *"Code: 497"* ]]; then
        echo "$output" >&2
        echo "Expected ACCESS_DENIED from query" >&2
        return 1
    fi
}

cleanup()
{
    for user_suffix in sealed invoker mv_caller root_mv_caller invoker mv_reader empty_security_caller; do
        user_id=$(qai_user_identifier "qai_${CLICKHOUSE_TEST_UNIQUE_NAME}_${user_suffix}")
        $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_id}" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

sealed_user=$(qai_user_identifier "qai_${CLICKHOUSE_TEST_UNIQUE_NAME}_sealed")
reader_user=$(qai_user_identifier "qai_${CLICKHOUSE_TEST_UNIQUE_NAME}_mv_reader")
caller_user=$(qai_user_identifier "qai_${CLICKHOUSE_TEST_UNIQUE_NAME}_empty_security_caller")

run_sql_input "$CUR_DIR"/05395_user_query_log_view_access_info.before.sql.in
expect_access_denied "EXECUTE AS ${sealed_user} SELECT count() FROM t_05395_private"
run_sql_input "$CUR_DIR"/05395_user_query_log_view_access_info.after.sql.in

run_sql_input "$CUR_DIR"/05395_nested_mv_access_info.sql.in

run_sql_input "$CUR_DIR"/05395_materialized_view_read.before.sql.in
expect_access_denied "EXECUTE AS ${reader_user} SELECT count() FROM t_05402_legacy_target"
run_sql_input "$CUR_DIR"/05395_materialized_view_read.after.sql.in

run_sql_input "$CUR_DIR"/05395_empty_security_mv_insert.before.sql.in
expect_access_denied "EXECUTE AS ${caller_user} INSERT INTO t_05401_source_sink SETTINGS log_comment = '05401_direct_source_sink_insert' SELECT toUInt64(1)"
expect_access_denied "EXECUTE AS ${caller_user} INSERT INTO t_05401_root_sink SETTINGS log_comment = '05401_direct_root_sink_insert' SELECT toUInt64(2)"
run_sql_input "$CUR_DIR"/05395_empty_security_mv_insert.after.sql.in
