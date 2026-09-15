#!/usr/bin/env bash
# An executable_pool worker whose process fails to start must not cost the pool a slot.
# The function below is configured with a pipe capacity that is out of range, so every
# attempt to start a worker fails. With a pool of one, the second call must still report
# that configuration error instead of waiting for a worker the pool can no longer create.
# Both calls have to run in one clickhouse-local process, because a fresh process gets a
# fresh pool.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/exec_pool_slot_leak_XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT

cat > "${WORK_DIR}/echo_udf.sh" << 'SCRIPT'
#!/usr/bin/env bash
while IFS= read -r x; do printf '%s\n' "$x"; done
SCRIPT
chmod +x "${WORK_DIR}/echo_udf.sh"

cat > "${WORK_DIR}/udf.xml" << EOF
<functions>
    <function>
        <type>executable_pool</type>
        <name>leak_udf</name>
        <return_type>UInt64</return_type>
        <argument><type>UInt64</type></argument>
        <format>TabSeparated</format>
        <command>echo_udf.sh</command>
        <execute_direct>1</execute_direct>
        <pool_size>1</pool_size>
        <max_command_execution_time>1</max_command_execution_time>
        <command_pipe_capacity>2147483648</command_pipe_capacity>
    </function>
</functions>
EOF

cat > "${WORK_DIR}/config.xml" << EOF
<clickhouse>
    <user_scripts_path>${WORK_DIR}/</user_scripts_path>
    <user_defined_executable_functions_config>${WORK_DIR}/udf.xml</user_defined_executable_functions_config>
</clickhouse>
EOF

# The last statement is deliberately not annotated so that its error reaches stderr.
cat > "${WORK_DIR}/queries.sql" << 'EOF'
SELECT leak_udf(1); -- { serverError UDF_EXECUTION_FAILED }
SELECT 'first call failed as expected';
SELECT leak_udf(2);
EOF

$CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" --queries-file="${WORK_DIR}/queries.sql" \
    < /dev/null > "${WORK_DIR}/out.txt" 2> "${WORK_DIR}/err.txt"

cat "${WORK_DIR}/out.txt"
grep -oE 'Pipe capacity [0-9]+ exceeds maximum supported value|Could not get process from pool' \
    "${WORK_DIR}/err.txt" | tail -1
