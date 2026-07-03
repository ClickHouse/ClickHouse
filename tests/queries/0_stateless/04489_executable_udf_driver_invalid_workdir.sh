#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# - no-fasttest: needs `cc` to compile a tiny C program
# - no-random-settings: the executable UDF path is server-set
#
# Regression test for driver-based executable UDF startup recovery when the `.workdir` sidecar is
# *corrupted* (not just missing). `reloadDriverBasedFunctions` re-runs the driver to rebuild lost
# dynamic state, but the recreate used to re-read the same corrupted sidecar and throw on the invalid
# UUID before invoking the driver, leaving the function permanently unavailable. Recovery must treat
# an unreadable previous sidecar as "no previous working directory" and regenerate the dynamic state.
#
# The SELECT results are matched via `grep` rather than exact stdout comparison because recovery logs
# the corrupted-sidecar read exception, and that diagnostic may surface on the client's stderr.

set -u

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Need `cc` available on the system to compile the user function body.
if ! command -v cc >/dev/null 2>&1; then
    echo "skipped: cc not available"
    cat "$CUR_DIR/04489_executable_udf_driver_invalid_workdir.reference"
    exit 0
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

DRIVER_DIR="$CUR_DIR/../../../programs/server/user_defined_executable_function_drivers"

cat > "$WORK_DIR/config.xml" <<EOF
<clickhouse>
    <allow_experimental_executable_udf_drivers>1</allow_experimental_executable_udf_drivers>
    <user_defined_executable_function_drivers_config>${WORK_DIR}/*_driver.xml</user_defined_executable_function_drivers_config>
    <dynamic_user_defined_executable_functions_path>${WORK_DIR}/dyn/</dynamic_user_defined_executable_functions_path>
    <user_defined_path>${WORK_DIR}/user_defined</user_defined_path>
    <user_scripts_path>${WORK_DIR}/user_scripts/</user_scripts_path>
    <path>${WORK_DIR}/data/</path>
</clickhouse>
EOF

cat > "$WORK_DIR/unsafe_c_driver.xml" <<EOF
<clickhouse>
    <driver>
        <name>UnsafeC</name>
        <create_command>${DRIVER_DIR}/unsafe_c_create.sh</create_command>
        <drop_command>${DRIVER_DIR}/unsafe_c_drop.sh</drop_command>
    </driver>
</clickhouse>
EOF

mkdir -p "$WORK_DIR/user_defined" "$WORK_DIR/user_scripts" "$WORK_DIR/dyn" "$WORK_DIR/data"

run() {
    "$CLICKHOUSE_LOCAL" --config-file="$WORK_DIR/config.xml" --query "$1" 2>&1
}

echo "-- create + call"
CREATE_OUTPUT=$(run "
CREATE FUNCTION test_udf_drv_recover ARGUMENTS (x UInt64, y UInt64) RETURNS UInt64
    ENGINE = UnsafeC() AS 'return x + y;';
SELECT test_udf_drv_recover(40, 2);
")
echo "$CREATE_OUTPUT" | grep -qx "42" && echo "created_result_42" || echo "BAD_CREATE_OUTPUT: $CREATE_OUTPUT"

ORIG_WORK_DIR_NAME=$(cat "$WORK_DIR/dyn/test_udf_drv_recover.workdir")

echo "-- corrupt the .workdir sidecar with an invalid uuid and drop the working directory"
echo "not-a-valid-uuid" > "$WORK_DIR/dyn/test_udf_drv_recover.workdir"
rm -rf "$WORK_DIR/dyn/$ORIG_WORK_DIR_NAME"

echo "-- recovery recreates the function despite the corrupted sidecar"
RECOVER_OUTPUT=$(run "SELECT test_udf_drv_recover(10, 5);")
echo "$RECOVER_OUTPUT" | grep -qx "15" && echo "recovered_result_15" || echo "BAD_RECOVER_OUTPUT: $RECOVER_OUTPUT"

echo "-- sidecar restored to a valid uuid pointing at an existing directory"
NEW_WORK_DIR_NAME=$(cat "$WORK_DIR/dyn/test_udf_drv_recover.workdir")
case "$NEW_WORK_DIR_NAME" in
    ????????-????-????-????-????????????) echo "uuid_workdir_name" ;;
    *) echo "bad_workdir_name:$NEW_WORK_DIR_NAME" ;;
esac
test -d "$WORK_DIR/dyn/$NEW_WORK_DIR_NAME" && echo "uuid_workdir_present" || echo "uuid_workdir_missing"

echo "-- drop removes everything"
run "DROP FUNCTION test_udf_drv_recover;" > /dev/null
test -f "$WORK_DIR/dyn/test_udf_drv_recover.xml" && echo "config_still_present" || echo "config_removed"
test -f "$WORK_DIR/dyn/test_udf_drv_recover.workdir" && echo "workdir_metadata_still_present" || echo "workdir_metadata_removed"
test -f "$WORK_DIR/user_defined/function_test_udf_drv_recover.sql" && echo "sql_still_present" || echo "sql_removed"
