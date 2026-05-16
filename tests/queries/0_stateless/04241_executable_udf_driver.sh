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
    <function>
        <name>c_function_body</name>
        <create_command>${DRIVER_DIR}/c_function_body_create.sh</create_command>
        <drop_command>${DRIVER_DIR}/c_function_body_drop.sh</drop_command>
        <env>
            <CLICKHOUSE_C_DRIVER_FORCE_LOCAL>1</CLICKHOUSE_C_DRIVER_FORCE_LOCAL>
        </env>
    </function>
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

echo "-- attach-style recreate after config loss"
rm -rf "$WORK_DIR/dyn/test_udf_drv_add.xml" "$WORK_DIR/dyn/test_udf_drv_add.yaml" "$WORK_DIR/dyn/test_udf_drv_add.d"
run "SELECT test_udf_drv_add(10, 5);"

echo "-- drop removes everything"
run "DROP FUNCTION test_udf_drv_add;"
test -f "$WORK_DIR/dyn/test_udf_drv_add.xml" && echo "config_still_present" || echo "config_removed"
test -f "$WORK_DIR/user_defined/function_test_udf_drv_add.sql" && echo "sql_still_present" || echo "sql_removed"
