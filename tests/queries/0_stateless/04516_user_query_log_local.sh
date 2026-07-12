#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

test_dir="${CLICKHOUSE_TMP}/04516_user_query_log_local_${CLICKHOUSE_DATABASE}"
config="${test_dir}/config.xml"

rm -rf "${test_dir}"
mkdir -p "${test_dir}/data" "${test_dir}/tmp" "${test_dir}/user_files" "${test_dir}/format_schemas"

make_config()
{
    cat > "${config}" <<EOF
<clickhouse>
    <path>${test_dir}/data/</path>
    <tmp_path>${test_dir}/tmp/</tmp_path>
    <user_files_path>${test_dir}/user_files/</user_files_path>
    <format_schema_path>${test_dir}/format_schemas/</format_schema_path>
    <logger>
        <level>none</level>
        <console>false</console>
    </logger>
    <query_log>
        <database>system</database>
        <table>$1</table>
        <engine>ENGINE = MergeTree PARTITION BY event_date ORDER BY event_time</engine>
        <enable_user_query_log>$2</enable_user_query_log>
    </query_log>
</clickhouse>
EOF
}

# Without a configured query log the table exists and is empty, not an error.
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM system.user_query_log"
${CLICKHOUSE_LOCAL} --query "SELECT engine FROM system.tables WHERE database = 'system' AND name = 'user_query_log'"

# With a configured query log the current user sees their own records.
make_config custom_query_log true
${CLICKHOUSE_LOCAL} --config-file "${config}" --log_queries 1 --query "
    SELECT 1 FORMAT Null;
    SYSTEM FLUSH LOGS query_log;
    SELECT count() >= 1, countIf(if(initial_user != '', initial_user, user) != currentUser()) FROM system.user_query_log;
"

# The table can be disabled.
make_config custom_query_log false
if ${CLICKHOUSE_LOCAL} --config-file "${config}" --query "SELECT count() FROM system.user_query_log" >/dev/null 2>&1
then
    echo "UNEXPECTED"
else
    echo "disabled"
fi

# The query log itself cannot be configured to flush into `system.user_query_log`.
make_config user_query_log true
if ${CLICKHOUSE_LOCAL} --config-file "${config}" --query "SELECT 1" 2>&1 | grep -q "cannot be set to"
then
    echo "config rejected"
else
    echo "UNEXPECTED"
fi

# A leftover table with this name (e.g. created before an upgrade) is reported on startup.
rm -rf "${test_dir}"
mkdir -p "${test_dir}/metadata/system"
echo "ATTACH DATABASE system ENGINE=Ordinary" > "${test_dir}/metadata/system.sql"
echo "ATTACH TABLE system.user_query_log (x UInt8) ENGINE = Memory;" > "${test_dir}/metadata/system/user_query_log.sql"
if ${CLICKHOUSE_LOCAL} --path "${test_dir}" --query "SELECT 1" 2>&1 | grep -q "already exists"
then
    echo "existing table reported"
else
    echo "UNEXPECTED"
fi

rm -rf "${test_dir}"
