#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The MergeTree readers may hand deserialization an array whose sizes stream is there while its
# elements stream is not, because they discard such a column and fill it with defaults afterwards.
# That is allowed only for the columns they actually refill - a column of a `Nested` type added by
# `ALTER`, read from the parts written before that `ALTER`. A column whose elements file is there
# but holds no data is not refilled, so it has to be rejected while reading instead of reaching the
# query pipeline with offsets that index past the end of its elements.

WORKING_DIR="${CLICKHOUSE_TMP}/05024_mergetree_array_missing_elements"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

# A `Nested` column added by `ALTER` reads its offsets from a sibling column and is filled with
# defaults: both part types keep working.
${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --query "
    CREATE TABLE wide (k UInt32, n Nested(a UInt32)) ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO wide SELECT number, range(number % 3) FROM numbers(5);
    ALTER TABLE wide ADD COLUMN n.b Array(UInt64);
    SELECT k, n.a, n.b FROM wide ORDER BY k;

    CREATE TABLE compact (k UInt32, n Nested(a UInt32)) ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = '1G';
    INSERT INTO compact SELECT number, range(number % 3) FROM numbers(5);
    ALTER TABLE compact ADD COLUMN n.b Array(UInt64);
    SELECT k, n.a, n.b FROM compact ORDER BY k;

    CREATE TABLE corrupted (k UInt32, a Array(UInt32)) ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO corrupted SELECT number, range(number % 3) FROM numbers(10);
"

# Empty the elements file of `a`, keeping its sizes file: the column is not recorded as partially
# read, so nothing refills it. The checksums are recalculated on the next load.
# `path` from `system.parts` ends with a slash.
# On a case-insensitive filesystem (e.g. macOS) every stream file is stored under the hash of its
# name (`replaceFileNameToHashIfNeeded`), so try both the plain name and the hashed one.
PART_DIR=$(${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'corrupted' AND active")
ELEMENTS_HASH=$(${CLICKHOUSE_LOCAL} --query "SELECT lower(hex(reverse(sipHash128('a'))))")
ELEMENTS_FILE=""
for CANDIDATE in "a.bin" "${ELEMENTS_HASH}.bin"
do
    [ -f "${PART_DIR}${CANDIDATE}" ] && ELEMENTS_FILE="${PART_DIR}${CANDIDATE}"
done
if [ -z "${ELEMENTS_FILE}" ]
then
    echo "no a.bin or ${ELEMENTS_HASH}.bin in the active part of table corrupted, parts:" >&2
    ${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --query "SELECT name, part_type, active, path FROM system.parts WHERE table = 'corrupted'" >&2
    find "${WORKING_DIR}" -type f >&2
    exit 1
fi
: > "${ELEMENTS_FILE}"
rm -f "${PART_DIR}checksums.txt"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --query "SELECT a FROM corrupted ORDER BY k" 2>&1 | grep -c -F 'INCORRECT_DATA'

rm -rf "${WORKING_DIR}"
