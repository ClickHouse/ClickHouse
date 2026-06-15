#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

test_dir="${CLICKHOUSE_TMP}/04340_user_query_log_reserved_name_ddl_${CLICKHOUSE_DATABASE}"
config="${test_dir}/config.xml"

rm -rf "${test_dir}"
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
        <enable_user_query_log>false</enable_user_query_log>
    </query_log>
</clickhouse>
EOF

if ${CLICKHOUSE_LOCAL} --config-file "${config}" --query "CREATE TABLE system.user_query_log (x UInt8) ENGINE = Memory" 2>&1 | grep -q "reserved for the filtered user query log view"
then
    echo "create reserved"
else
    echo "UNEXPECTED"
fi

${CLICKHOUSE_LOCAL} --config-file "${config}" --query "CREATE TABLE t_04340 (x UInt8) ENGINE = Memory"

if ${CLICKHOUSE_LOCAL} --config-file "${config}" --query "RENAME TABLE t_04340 TO system.user_query_log" 2>&1 | grep -q "reserved for the filtered user query log view"
then
    echo "rename reserved"
else
    echo "UNEXPECTED"
fi
