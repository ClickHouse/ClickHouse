#!/usr/bin/env bash

# What a function does about the command's stderr and exit code, the capacity of its pipes and
# whether it uses the shared-memory transport are part of its configuration, and they have to be
# readable from `system.user_defined_functions`: one function spells out non-default values, the
# other gets the defaults (`log_last`, exit code checked, no shared memory). The functions are only
# loaded, never called.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/udf_system_table_XXXXXX")
trap 'rm -rf "${CONFIG_DIR}"' EXIT

cat > "${CONFIG_DIR}/functions.xml" <<'XML'
<functions>
    <function>
        <type>executable</type>
        <name>udf_explicit</name>
        <return_type>String</return_type>
        <argument><type>UInt64</type></argument>
        <format>TabSeparated</format>
        <command>cat</command>
        <stderr_reaction>throw</stderr_reaction>
        <check_exit_code>0</check_exit_code>
        <command_pipe_capacity>131072</command_pipe_capacity>
    </function>
    <function>
        <type>executable_pool</type>
        <name>udf_defaults</name>
        <return_type>String</return_type>
        <argument><type>UInt64</type></argument>
        <format>TabSeparated</format>
        <command>cat</command>
    </function>
</functions>
XML

$CLICKHOUSE_LOCAL --query "
    SELECT name, load_status, stderr_reaction, check_exit_code, command_pipe_capacity,
        use_shared_memory, shared_memory_size, shared_memory_max_size
    FROM system.user_defined_functions WHERE name LIKE 'udf\\_%' ORDER BY name;
" -- --user_defined_executable_functions_config="${CONFIG_DIR}/functions.xml"
