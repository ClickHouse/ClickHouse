#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e
CONFIG_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/udf_failed_config_XXXXXX")
trap 'rm -rf "$CONFIG_DIR"' EXIT

# A function that failed to load has no configuration to report. The columns that predate the
# shared-memory transport keep their types and show their defaults, as they always have (`load_status`
# says they mean nothing). The columns added with the transport are `NULL`, so that a failed
# `executable_pool` with `use_shared_memory` does not look like one configured for the pipes.
cat > "$CONFIG_DIR/functions.xml" <<'XML'
<functions>
    <function>
        <type>executable_pool</type>
        <name>failed_pool</name>
        <return_type>InvalidTypeForFailedUDF</return_type>
        <argument><type>UInt64</type><name>value</name></argument>
        <format>TabSeparated</format>
        <command>cat</command>
        <pool_size>4</pool_size>
        <use_shared_memory>1</use_shared_memory>
        <shared_memory_size>65536</shared_memory_size>
        <shared_memory_max_size>131072</shared_memory_max_size>
    </function>
    <function>
        <type>executable</type>
        <name>loaded_pipe</name>
        <return_type>String</return_type>
        <argument><type>UInt64</type></argument>
        <format>TabSeparated</format>
        <command>cat</command>
        <check_exit_code>0</check_exit_code>
    </function>
    <function>
        <type>executable_pool</type>
        <name>loaded_pool</name>
        <return_type>String</return_type>
        <argument><type>UInt64</type><name>value</name></argument>
        <format>TabSeparated</format>
        <command>cat</command>
        <pool_size>4</pool_size>
    </function>
</functions>
XML

$CLICKHOUSE_LOCAL --query "
    SELECT name, load_status, type, pool_size, use_shared_memory, shared_memory_size,
        shared_memory_max_size, check_exit_code, stderr_reaction
    FROM system.user_defined_functions ORDER BY name;

    SELECT NOT empty(loading_error_message),
        arrayAll(x -> empty(x), [command, format, return_type, return_name]),
        arrayAll(x -> x = 0, [max_command_execution_time, command_termination_timeout,
            command_read_timeout, command_write_timeout, pool_size, send_chunk_header,
            execute_direct, lifetime, deterministic]),
        isNull(stderr_reaction),
        arrayAll(x -> isNull(x), [check_exit_code, use_shared_memory,
            shared_memory_size, shared_memory_max_size, command_pipe_capacity]),
        argument_types, argument_names
    FROM system.user_defined_functions WHERE name = 'failed_pool';
" -- --user_defined_executable_functions_config="$CONFIG_DIR/functions.xml"
