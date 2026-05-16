#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# - no-fasttest: needs `cc` to compile a tiny C program
# - no-parallel: writes to dirs that are not unique per test
# - no-random-settings: the executable UDF path is server-set
#
# This test exercises the executable UDF "driver" feature end-to-end via clickhouse-local:
#   1. Define a driver in a config file
#   2. CREATE FUNCTION ... ENGINE = c_function_body() AS '...'
#   3. Call the created function
#   4. Simulate the driver-generated config file disappearing and verify it is recreated
#   5. DROP FUNCTION and verify cleanup

set -u

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Need `cc` available on the system to compile the user function body.
if ! command -v cc >/dev/null 2>&1; then
    echo "skipped: cc not available"
    cat "$CUR_DIR/04241_executable_udf_driver.reference"
    exit 0
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

DRIVER_DIR="$CUR_DIR/../../../programs/server/user_defined_executable_function_drivers"

cat > "$WORK_DIR/config.xml" <<EOF
<clickhouse>
    <user_defined_executable_function_drivers_config>${WORK_DIR}/c_function_body_driver.xml</user_defined_executable_function_drivers_config>
    <dynamic_user_defined_executable_functions_path>${WORK_DIR}/dyn/</dynamic_user_defined_executable_functions_path>
    <user_defined_path>${WORK_DIR}/user_defined</user_defined_path>
    <user_scripts_path>${WORK_DIR}/user_scripts/</user_scripts_path>
    <path>${WORK_DIR}/data/</path>
</clickhouse>
EOF

cat > "$WORK_DIR/c_function_body_driver.xml" <<EOF
<clickhouse>
    <driver>
        <name>c_function_body</name>
        <create_command>${DRIVER_DIR}/c_function_body_create.sh</create_command>
        <drop_command>${DRIVER_DIR}/c_function_body_drop.sh</drop_command>
        <env>
            <CLICKHOUSE_C_DRIVER_FORCE_LOCAL>1</CLICKHOUSE_C_DRIVER_FORCE_LOCAL>
        </env>
    </driver>
</clickhouse>
EOF

mkdir -p "$WORK_DIR/user_defined" "$WORK_DIR/user_scripts" "$WORK_DIR/dyn" "$WORK_DIR/data"

run() {
    # Filter out shutdown-related noise (e.g. shell-printed "Segmentation fault" lines from
    # the bash that invoked clickhouse-local) so we test what the queries actually produced.
    "$CLICKHOUSE_LOCAL" --config-file="$WORK_DIR/config.xml" --query "$1" 2>&1 \
        | grep -v -E '^/bin/bash|Segmentation fault' || true
}

echo "-- create + call"
run "
CREATE FUNCTION test_udf_drv_add ARGUMENTS (x UInt8, y UInt8) RETURNS Int64
    ENGINE = c_function_body() AS 'return (int64_t) x + (int64_t) y;';
SELECT test_udf_drv_add(40, 2);
"

echo "-- dynamic config exists after create"
test -f "$WORK_DIR/dyn/test_udf_drv_add.xml" && echo "yes" || echo "no"
grep -q '<format>Buffers</format>' "$WORK_DIR/dyn/test_udf_drv_add.xml" && echo "buffers_format" || echo "bad_format"
grep -q '<send_chunk_header>1</send_chunk_header>' "$WORK_DIR/dyn/test_udf_drv_add.xml" && echo "chunk_header_enabled" || echo "chunk_header_disabled"

echo "-- work dir uses uuid name"
WORK_DIR_NAME=$(cat "$WORK_DIR/dyn/test_udf_drv_add.workdir")
case "$WORK_DIR_NAME" in
    ????????-????-????-????-????????????) echo "uuid_workdir_name" ;;
    *) echo "bad_workdir_name:$WORK_DIR_NAME" ;;
esac
test -d "$WORK_DIR/dyn/$WORK_DIR_NAME" && echo "uuid_workdir_present" || echo "uuid_workdir_missing"
test -d "$WORK_DIR/dyn/test_udf_drv_add.d" && echo "function_workdir_present" || echo "function_workdir_absent"
grep -q 'setvbuf' "$WORK_DIR/dyn/$WORK_DIR_NAME/wrapper.c" && echo "setvbuf_present" || echo "setvbuf_absent"
grep -Eq 'fread|fwrite|fgets|fflush|fputs|scanf|printf|strtoull|setvbuf' "$WORK_DIR/dyn/$WORK_DIR_NAME/wrapper.c" && echo "stdio_serde_present" || echo "stdio_serde_absent"
grep -q 'ch_arg_0_values\[row\]' "$WORK_DIR/dyn/$WORK_DIR_NAME/wrapper.c" && echo "numeric_arg_view_present" || echo "numeric_arg_view_missing"
grep -q 'ch_result_values\[row\] = user_function' "$WORK_DIR/dyn/$WORK_DIR_NAME/wrapper.c" && echo "numeric_result_view_present" || echo "numeric_result_view_missing"
grep -q 'static CH_ALWAYS_INLINE int ch_process_chunk' "$WORK_DIR/dyn/$WORK_DIR_NAME/wrapper.c" && echo "chunk_loop_function_present" || echo "chunk_loop_function_missing"
grep -q 'ch_input_buffer' "$WORK_DIR/dyn/$WORK_DIR_NAME/wrapper.c" && echo "input_staging_present" || echo "input_staging_absent"

echo "-- multi-row chunk call"
run "SELECT sum(test_udf_drv_add(toUInt8(number), toUInt8(1))) FROM numbers(10);"

echo "-- declared argument cast"
run "
CREATE FUNCTION test_udf_drv_add_u64 ARGUMENTS (x UInt64, y UInt64) RETURNS UInt64
    ENGINE = c_function_body() AS 'return x + y;';
SELECT test_udf_drv_add_u64(1, 2);
"

echo "-- string args + return"
run "
CREATE FUNCTION test_udf_drv_concat ARGUMENTS (x String, y String) RETURNS String
    ENGINE = c_function_body() AS '
        struct buf out = alloc(x.size + y.size);
        if (out.data == NULL && out.size != 0)
            return out;
        char * p = (char *)out.data;
        for (size_t i = 0; i != x.size; ++i)
            p[i] = x.data[i];
        for (size_t i = 0; i != y.size; ++i)
            p[x.size + i] = y.data[i];
        return out;
    ';
SELECT test_udf_drv_concat('hello ', 'world');
SELECT length(test_udf_drv_concat('', ''));
SELECT arrayStringConcat(groupArray(value), '') FROM
(
    SELECT test_udf_drv_concat(toString(number), ':') AS value
    FROM numbers(5)
    ORDER BY number
);
"
STRING_WORK_DIR_NAME=$(cat "$WORK_DIR/dyn/test_udf_drv_concat.workdir")
grep -q 'struct buf alloc(size_t size)' "$WORK_DIR/dyn/$STRING_WORK_DIR_NAME/wrapper.c" && echo "string_allocator_present" || echo "string_allocator_missing"
grep -Eq 'fread|fwrite|fgets|fflush|fputs|scanf|printf|strtoull|setvbuf' "$WORK_DIR/dyn/$STRING_WORK_DIR_NAME/wrapper.c" && echo "string_stdio_serde_present" || echo "string_stdio_serde_absent"

echo "-- if not exists does not invoke driver"
run "
CREATE FUNCTION IF NOT EXISTS test_udf_drv_add ARGUMENTS (x UInt8, y UInt8) RETURNS Int64
    ENGINE = c_function_body() AS 'this is not valid C code';
SELECT test_udf_drv_add(1, 2);
"

echo "-- attach-style recreate after config loss"
rm -rf "$WORK_DIR/dyn/test_udf_drv_add.xml" "$WORK_DIR/dyn/test_udf_drv_add.yaml" "$WORK_DIR/dyn/test_udf_drv_add.workdir" "$WORK_DIR/dyn/$WORK_DIR_NAME"
run "SELECT test_udf_drv_add(10, 5);"
RECREATED_WORK_DIR_NAME=$(cat "$WORK_DIR/dyn/test_udf_drv_add.workdir")

echo "-- drop removes everything"
run "DROP FUNCTION test_udf_drv_add; DROP FUNCTION test_udf_drv_add_u64; DROP FUNCTION test_udf_drv_concat;"
test -f "$WORK_DIR/dyn/test_udf_drv_add.xml" && echo "config_still_present" || echo "config_removed"
test -f "$WORK_DIR/dyn/test_udf_drv_add.workdir" && echo "workdir_metadata_still_present" || echo "workdir_metadata_removed"
test -d "$WORK_DIR/dyn/$RECREATED_WORK_DIR_NAME" && echo "workdir_still_present" || echo "workdir_removed"
test -f "$WORK_DIR/user_defined/function_test_udf_drv_add.sql" && echo "sql_still_present" || echo "sql_removed"
test -f "$WORK_DIR/dyn/test_udf_drv_add_u64.xml" && echo "u64_config_still_present" || echo "u64_config_removed"
test -f "$WORK_DIR/dyn/test_udf_drv_add_u64.workdir" && echo "u64_workdir_metadata_still_present" || echo "u64_workdir_metadata_removed"
test -f "$WORK_DIR/user_defined/function_test_udf_drv_add_u64.sql" && echo "u64_sql_still_present" || echo "u64_sql_removed"
test -f "$WORK_DIR/dyn/test_udf_drv_concat.xml" && echo "string_config_still_present" || echo "string_config_removed"
test -f "$WORK_DIR/dyn/test_udf_drv_concat.workdir" && echo "string_workdir_metadata_still_present" || echo "string_workdir_metadata_removed"
test -d "$WORK_DIR/dyn/$STRING_WORK_DIR_NAME" && echo "string_workdir_still_present" || echo "string_workdir_removed"
test -f "$WORK_DIR/user_defined/function_test_udf_drv_concat.sql" && echo "string_sql_still_present" || echo "string_sql_removed"
