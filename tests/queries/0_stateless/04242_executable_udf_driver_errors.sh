#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
#
# Verify the error path of executable UDF drivers:
#   - A non-zero exit code is surfaced to the user
#   - The full stderr produced by the driver is included in the exception message
#   - No leftover state (working dir, SQL metadata, dynamic config) is persisted

set -u

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

# A failing driver: writes two lines to stderr and exits 17.
cat > "$WORK_DIR/fail_driver.sh" <<'EOF'
#!/bin/sh
echo "DRIVER_STDERR_LINE_1: synthetic failure" >&2
echo "DRIVER_STDERR_LINE_2: second line" >&2
exit 17
EOF
chmod +x "$WORK_DIR/fail_driver.sh"

# A driver that exits 0 but emits nothing on stdout.
cat > "$WORK_DIR/silent_driver.sh" <<'EOF'
#!/bin/sh
exit 0
EOF
chmod +x "$WORK_DIR/silent_driver.sh"

cat > "$WORK_DIR/drivers.xml" <<EOF
<clickhouse>
    <driver>
        <name>fail_driver</name>
        <create_command>${WORK_DIR}/fail_driver.sh</create_command>
    </driver>
    <driver>
        <name>silent_driver</name>
        <create_command>${WORK_DIR}/silent_driver.sh</create_command>
    </driver>
</clickhouse>
EOF

cat > "$WORK_DIR/config.xml" <<EOF
<clickhouse>
    <user_defined_executable_function_drivers_config>${WORK_DIR}/drivers.xml</user_defined_executable_function_drivers_config>
    <dynamic_user_defined_executable_functions_path>${WORK_DIR}/dyn/</dynamic_user_defined_executable_functions_path>
    <user_defined_path>${WORK_DIR}/user_defined</user_defined_path>
    <user_scripts_path>${WORK_DIR}/user_scripts/</user_scripts_path>
    <path>${WORK_DIR}/data/</path>
</clickhouse>
EOF

mkdir -p "$WORK_DIR/user_defined" "$WORK_DIR/user_scripts" "$WORK_DIR/dyn" "$WORK_DIR/data"

run() {
    "$CLICKHOUSE_LOCAL" --config-file="$WORK_DIR/config.xml" --query "$1" 2>&1 \
        | grep -v -E '^/bin/bash|Segmentation fault' || true
}

# Confirm that the exception message contains both the exit code and the stderr lines.
expected_marker_1="exited with code 17"
expected_marker_2="DRIVER_STDERR_LINE_1: synthetic failure"
expected_marker_3="DRIVER_STDERR_LINE_2: second line"

OUTPUT=$(run "CREATE FUNCTION fn_bad ARGUMENTS (x UInt8) RETURNS Int64 ENGINE = fail_driver() AS 'whatever';")
if echo "$OUTPUT" | grep -q "$expected_marker_1" \
    && echo "$OUTPUT" | grep -q "$expected_marker_2" \
    && echo "$OUTPUT" | grep -q "$expected_marker_3"; then
    echo "non_zero_exit_propagated"
else
    echo "MISSING_MARKERS in: $OUTPUT"
fi

# Driver failure must not leave any persisted state behind.
LEFTOVER_WORKDIRS=$(find "$WORK_DIR/dyn" -mindepth 1 -maxdepth 1 -type d ! -name preprocessed_configs | wc -l)
test "$LEFTOVER_WORKDIRS" -eq 0 && echo "workdir_clean" || echo "workdir_leaked"
test -f "$WORK_DIR/dyn/fn_bad.xml" && echo "config_leaked" || echo "config_clean"
test -f "$WORK_DIR/user_defined/function_fn_bad.sql" && echo "sql_leaked" || echo "sql_clean"

# Driver exiting zero but emitting no config is also an error - the message should mention it.
OUTPUT2=$(run "CREATE FUNCTION fn_silent ARGUMENTS (x UInt8) RETURNS Int64 ENGINE = silent_driver() AS '';")
if echo "$OUTPUT2" | grep -q "empty configuration"; then
    echo "empty_output_detected"
else
    echo "MISSING_EMPTY_MARKER in: $OUTPUT2"
fi

# Unknown driver should produce a clear error.
OUTPUT3=$(run "CREATE FUNCTION fn_unknown ARGUMENTS (x UInt8) RETURNS Int64 ENGINE = nope_driver() AS '';")
if echo "$OUTPUT3" | grep -q "is not registered"; then
    echo "unknown_driver_detected"
else
    echo "MISSING_UNKNOWN_MARKER in: $OUTPUT3"
fi
