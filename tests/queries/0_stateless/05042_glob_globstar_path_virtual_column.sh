#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes into user_files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap "rm -rf '${DATA_DIR}' 2>/dev/null" EXIT
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

# The same file, reached by a pattern whose expansion leaves no wildcard. That takes the
# exact-match branch instead of the directory iterator, so the two branches are covered
# separately.
$CLICKHOUSE_CLIENT -q "
SELECT id FROM file('${DATA_DIR}/link/{..,missing}/x.csv', 'CSV', 'id UInt64');
"
