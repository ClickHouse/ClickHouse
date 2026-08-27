#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes into user_files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
OUTSIDE_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_outside"
trap "rm -rf '${DATA_DIR}' '${OUTSIDE_DIR}' 2>/dev/null" EXIT
rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}/country=PT/yr=2024"
printf '0\n1\n2\n' > "${DATA_DIR}/country=PT/yr=2024/a.csv"

FILE="${DATA_DIR}/country=PT/yr=2024/a.csv"

# `_path` names the file, so it must not depend on the glob syntax that reached it.
$CLICKHOUSE_CLIENT -q "
SELECT DISTINCT _path = '${FILE}' FROM file('${DATA_DIR}/**/*.csv', 'CSV', 'id UInt64');
"

# The predicate a user writes against the real path.
$CLICKHOUSE_CLIENT -q "
SELECT count() FROM file('${DATA_DIR}/**/*.csv', 'CSV', 'id UInt64') WHERE _path = '${FILE}';
"

# Cross-syntax agreement: the same file reached two ways yields one `_path`. This is the
# invariant, and it distinguishes the fix from a normalization that happens to look right.
$CLICKHOUSE_CLIENT -q "
SELECT uniqExact(p) FROM (
    SELECT DISTINCT _path AS p FROM file('${DATA_DIR}/**/*.csv', 'CSV', 'id UInt64')
    UNION DISTINCT
    SELECT DISTINCT _path AS p FROM file('${DATA_DIR}/*/*/*.csv', 'CSV', 'id UInt64')
);
"

# A `..` behind a symlink resolves to the symlink's target, not to its parent, and symlinks
# under user_files are supported. So a matched path must keep its `..`: the row read here
# comes from `deep/x.csv` (111) and not from the same-named file one level up (222). A `..`
# only reaches this point through brace expansion, which runs after the pattern is normalized.
mkdir -p "${DATA_DIR}/deep/target"
printf '111\n' > "${DATA_DIR}/deep/x.csv"
printf '222\n' > "${DATA_DIR}/x.csv"
ln -s "${DATA_DIR}/deep/target" "${DATA_DIR}/link"

$CLICKHOUSE_CLIENT -q "
SELECT id FROM file('${DATA_DIR}/link/{..,missing}/*.csv', 'CSV', 'id UInt64');
"

# When the expansion leaves no wildcard, the exact-match branch folds the `..` away, so the
# path names the lexical sibling (222) rather than the symlink's target. That fold is what keeps
# the path passed to the access-control check identical to the one that is opened.
$CLICKHOUSE_CLIENT -q "
SELECT id FROM file('${DATA_DIR}/link/{..,missing}/x.csv', 'CSV', 'id UInt64');
"

# A `..` behind a symlink must not widen the sandbox. `esc` points outside user_files, so a `..`
# that survived expansion would name the target's parent there, while the access-control check,
# which normalizes its own copy, would only ever see an in-sandbox path. The row is readable on
# disk, so printing it would mean the read escaped; the count must stay 0.
rm -rf "${OUTSIDE_DIR}"
mkdir -p "${OUTSIDE_DIR}/inner"
printf '999\n' > "${OUTSIDE_DIR}/secret.csv"
ln -s "${OUTSIDE_DIR}/inner" "${DATA_DIR}/esc"
# Printed as the path the query resolved to, so an unrelated failure cannot pass as a denial: the
# `..` has to be gone and `esc/` with it. The forwarded server log would repeat the same text, so
# only the client's own exception is read here.
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal -q "
SELECT count() FROM file('${DATA_DIR}/esc/{..,missing}/secret.csv', 'CSV', 'id UInt64') WHERE id = 999;
" 2>&1 | sed -n "s|.*File ${DATA_DIR}/\([^ ]*\) doesn't exist.*|resolved to \1|p"

# The post-read rename must move the file that was read, not a namesake elsewhere.
RENAME_DIR="${DATA_DIR}/rename"
mkdir -p "${RENAME_DIR}/deep/target"
printf '111\n' > "${RENAME_DIR}/deep/x.csv"
printf '222\n' > "${RENAME_DIR}/x.csv"
ln -s "${RENAME_DIR}/deep/target" "${RENAME_DIR}/link"

$CLICKHOUSE_CLIENT -q "
SELECT id FROM file('${RENAME_DIR}/link/{..,missing}/x.csv', 'CSV', 'id UInt64')
SETTINGS rename_files_after_processing = 'processed_%f%e';
"
[ -f "${RENAME_DIR}/processed_x.csv" ] && echo "renamed the file that was read"
[ -f "${RENAME_DIR}/deep/x.csv" ] && echo "the symlinked namesake was left alone"

# Same rename, reached through the directory iterator instead. Here the matched path keeps its
# `..`, so the file read lives under the symlink's target and the rename must land there too.
WREN_DIR="${DATA_DIR}/wrename"
mkdir -p "${WREN_DIR}/deep/target"
printf '111\n' > "${WREN_DIR}/deep/x.csv"
printf '222\n' > "${WREN_DIR}/x.csv"
ln -s "${WREN_DIR}/deep/target" "${WREN_DIR}/link"

$CLICKHOUSE_CLIENT -q "
SELECT id FROM file('${WREN_DIR}/link/{..,missing}/*.csv', 'CSV', 'id UInt64')
SETTINGS rename_files_after_processing = 'processed_%f%e';
"
[ -f "${WREN_DIR}/deep/processed_x.csv" ] && echo "renamed beside the file that was read"
[ -f "${WREN_DIR}/x.csv" ] && echo "the lexical sibling was left alone"

# The rename must not reach outside user_files either. A wildcard keeps the `..`, so the file read
# here lives under the symlink's target; the access check resolves the directory before approving
# it. The row count shows the read happened, which is what selects the rename path, so the outside
# file keeping its name is not just a query that never ran.
# The refusal is reported by the reader's destructor, which cannot throw, so it reaches the client
# as a forwarded server log rather than as a query error.
ln -s "${OUTSIDE_DIR}/inner" "${DATA_DIR}/escw"
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal -q "
SELECT count() FROM file('${DATA_DIR}/escw/{..,missing}/*.csv', 'CSV', 'id UInt64')
SETTINGS rename_files_after_processing = 'processed_%f%e';
"
[ -f "${OUTSIDE_DIR}/secret.csv" ] && echo "the file outside user_files kept its name"

# A directory symlink leaving user_files stays readable and renameable, which is what the check
# above must not take away: without a `..` the path is passed to it exactly as written.
mkdir -p "${OUTSIDE_DIR}/plain"
printf '42\n' > "${OUTSIDE_DIR}/plain/f.csv"
ln -s "${OUTSIDE_DIR}/plain" "${DATA_DIR}/out"
$CLICKHOUSE_CLIENT -q "
SELECT id FROM file('${DATA_DIR}/out/*.csv', 'CSV', 'id UInt64')
SETTINGS rename_files_after_processing = 'processed_%f%e';
"
[ -f "${OUTSIDE_DIR}/plain/processed_f.csv" ] && echo "the symlinked directory is still renameable"
