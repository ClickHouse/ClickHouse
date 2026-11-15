#!/usr/bin/env bash
# Test that RESTORE rejects backup entries with path traversal sequences (../)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tbl_backup_traversal"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE tbl_backup_traversal (id UInt64, data String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO tbl_backup_traversal VALUES (1, 'hello')"

backups_disk_root=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name='backups'" 2>/dev/null)

if [ -z "${backups_disk_root}" ]; then
    echo "backups disk is not configured, skipping test"
    exit 0
fi

extra_content="EXTRA_FILE_CONTENT_HERE"
extra_size=${#extra_content}
extra_checksum=$(echo -n "${extra_content}" | md5sum | awk '{print $1}')
extra_data_path="data/default/tbl_backup_traversal/extra_payload.bin"

# Creates a backup, injects an extra file entry into its .backup metadata, and
# attempts to restore. Expects the specified error.
#   $1 - backup suffix
#   $2 - injected <name> value
#   $3 - expected error code (e.g. INSECURE_PATH, BACKUP_DAMAGED)
#   $4 - (optional) injected <data_file> value; defaults to extra_data_path
#   $5 - (optional) space-separated companion <name> values, each getting its own entry with
#        the same size, checksum and data file: a name that RESTORE reads back under a
#        different key needs that key too
inject_and_restore() {
    local suffix="$1"
    local injected_name="$2"
    local expected_error="$3"
    local injected_data_file="${4:-${extra_data_path}}"
    local companion_names="${5:-}"
    local bname="${CLICKHOUSE_TEST_UNIQUE_NAME}_${suffix}"

    ${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl_backup_traversal TO Disk('backups', '${bname}')" > /dev/null 2>&1

    local bpath="${backups_disk_root}/${bname}"
    mkdir -p "${bpath}/$(dirname "${extra_data_path}")"
    echo -n "${extra_content}" > "${bpath}/${extra_data_path}"

    local injected_entries=""
    local name
    # shellcheck disable=SC2086
    for name in "${injected_name}" ${companion_names}; do
        injected_entries="${injected_entries}<file><name>${name}</name><size>${extra_size}</size><checksum>${extra_checksum}</checksum><data_file>${injected_data_file}</data_file></file>"
    done

    sed -i "s|</contents>|${injected_entries}</contents>|" "${bpath}/.backup"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tbl_backup_traversal"
    ${CLICKHOUSE_CLIENT} -m -q "RESTORE TABLE tbl_backup_traversal FROM Disk('backups', '${bname}'); -- { serverError ${expected_error} }"
}

# Recreates the table from scratch between tests. The drop is required: test 8 leaves a
# restored table behind, and inserting into it would add a second part, so the part that
# tests 9 and 10 address would no longer be the only part a backup taken here contains.
recreate_table() {
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tbl_backup_traversal SYNC"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE tbl_backup_traversal (id UInt64, data String) ENGINE = MergeTree ORDER BY id"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO tbl_backup_traversal VALUES (1, 'hello')"
}

# Tests 9 and 10 need an entry that RESTORE actually lists, so their names must address a real
# part of the table this test backs up.
active_part() {
    ${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'tbl_backup_traversal' AND active"
}

# Test 1: relative path traversal in <name>.
inject_and_restore "rel" "data/default/tbl_backup_traversal/all_0_0_0/../../../../../../../tmp/backup_traversal_test_output.txt" INSECURE_PATH

# Verify the file was NOT written outside the backup directory.
if [ -f "/tmp/backup_traversal_test_output.txt" ]; then
    echo "FAIL: file written to /tmp/"
    rm -f "/tmp/backup_traversal_test_output.txt"
else
    echo "OK: path traversal was blocked"
fi

# Test 2: absolute path in <name>.
recreate_table
inject_and_restore "abs" "/tmp/backup_absolute_path_test_output.xml" INSECURE_PATH

# Test 3: path traversal in <data_file> (source path for reading from the backup).
recreate_table
inject_and_restore "datafile" "data/default/tbl_backup_traversal/extra_payload.bin" INSECURE_PATH "data/default/tbl_backup_traversal/all_0_0_0/../../../../../../../etc/passwd"

# Test 4: empty <name> should be rejected as damaged.
recreate_table
inject_and_restore "empty" "" BACKUP_DAMAGED

# Test 5: "." as <name> should be rejected as damaged.
recreate_table
inject_and_restore "dot" "." BACKUP_DAMAGED

# Test 6: bare ".." as <name>.
recreate_table
inject_and_restore "dotdot" ".." INSECURE_PATH

# Test 7: absolute path in <data_file>.
recreate_table
inject_and_restore "abs_datafile" "data/default/tbl_backup_traversal/extra_payload.bin" INSECURE_PATH "/etc/passwd"

# Test 8: normal backup/restore still works after the validation was added.
recreate_table
normal_backup="${CLICKHOUSE_TEST_UNIQUE_NAME}_normal"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl_backup_traversal TO Disk('backups', '${normal_backup}')" > /dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "DROP TABLE tbl_backup_traversal"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE tbl_backup_traversal FROM Disk('backups', '${normal_backup}')" > /dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT * FROM tbl_backup_traversal"

# Test 9: a doubled separator in <name>. The name normalizes to a path inside the backup, so
# tests 1-7 do not catch it, and the part's file list then held the rooted remainder "/tmp/...".
# The companion entry is what makes the case bite: RESTORE reads the file back under that key.
recreate_table
double_slash_target="/tmp/backup_double_slash_test_output.txt"
rm -f "${double_slash_target}"
inject_and_restore "double_slash" \
    "data/${CLICKHOUSE_DATABASE}/tbl_backup_traversal/$(active_part)//tmp/backup_double_slash_test_output.txt" \
    INSECURE_PATH "" "tmp/backup_double_slash_test_output.txt"

if [ -f "${double_slash_target}" ]; then
    echo "FAIL: file written outside the part directory"
    rm -f "${double_slash_target}"
else
    echo "OK: doubled separator was blocked"
fi

# Test 10: as many ".." as the four components of the entry's directory prefix. The name still
# normalizes to a path inside the backup, but the remainder escapes the part's destination
# directory, which sits four components below the disk root.
recreate_table
default_disk_root=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name='default'")
shallow_target="${default_disk_root%/}/tmp/backup_shallow_dotdot_test_output.txt"
rm -f "${shallow_target}"
inject_and_restore "shallow_dotdot" \
    "data/${CLICKHOUSE_DATABASE}/tbl_backup_traversal/$(active_part)/../../../../tmp/backup_shallow_dotdot_test_output.txt" \
    INSECURE_PATH

if [ -f "${shallow_target}" ]; then
    echo "FAIL: file written outside the part directory"
    rm -f "${shallow_target}"
else
    echo "OK: shallow traversal was blocked"
fi

# Test 11: a table with a projection still round-trips. Projection files are the legitimate
# entries that carry a subdirectory inside the part ("<part>/p.proj/columns.txt").
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tbl_backup_projection SYNC"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE tbl_backup_projection (id UInt64, data String, PROJECTION p (SELECT data, count() GROUP BY data)) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO tbl_backup_projection VALUES (1, 'a'), (2, 'b'), (3, 'a')"
projection_backup="${CLICKHOUSE_TEST_UNIQUE_NAME}_projection"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl_backup_projection TO Disk('backups', '${projection_backup}')" > /dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "DROP TABLE tbl_backup_projection SYNC"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE tbl_backup_projection FROM Disk('backups', '${projection_backup}')" > /dev/null 2>&1
${CLICKHOUSE_CLIENT} --query "SELECT data, count() FROM tbl_backup_projection GROUP BY data ORDER BY data"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 'tbl_backup_projection' AND active"

# Clean up.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tbl_backup_traversal"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tbl_backup_projection SYNC"
rm -rf "${backups_disk_root:?}/${CLICKHOUSE_TEST_UNIQUE_NAME}"_* 2>/dev/null || true
