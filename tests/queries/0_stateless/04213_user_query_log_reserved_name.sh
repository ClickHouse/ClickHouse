#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

test_dir="${CLICKHOUSE_TMP}/04213_user_query_log_reserved_name_${CLICKHOUSE_DATABASE}"
config="${test_dir}/config.xml"

mkdir -p "${test_dir}/data" "${test_dir}/tmp" "${test_dir}/user_files" "${test_dir}/format_schemas"

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
        <table>user_query_log</table>
        <enable_user_query_log>false</enable_user_query_log>
        <engine>ENGINE = MergeTree PARTITION BY event_date ORDER BY event_time</engine>
    </query_log>
</clickhouse>
EOF

if ${CLICKHOUSE_LOCAL} --config-file "${config}" --query "SELECT 1" 2>&1 | grep -q "reserved for the filtered user query log view"
then
    echo "reserved"
else
    echo "UNEXPECTED"
fi
