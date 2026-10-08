#!/usr/bin/env bash
# Tags: log-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Log` table whose array sizes are there while the array elements are not describes elements that
# cannot be read. Such a column used to be returned with offsets that index past the end of its
# elements, and the first consumer that indexed it - `ORDER BY` here - read out of bounds. Only the
# MergeTree readers may see a partially read column, because they throw it away and fill it with
# defaults afterwards; everywhere else the data has to be rejected while reading it.

WORKING_DIR="${CLICKHOUSE_TMP}/05099_log_array_missing_elements"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --query "
    CREATE TABLE t (k UInt32, a Array(UInt32), m Map(String, UInt32)) ENGINE = Log;
    INSERT INTO t SELECT number, range(number % 3), map('x', number, 'y', number) FROM numbers(10);
"

# Drop the elements streams of both columns, keeping their recorded file sizes in sync, and leave the
# sizes streams alone.
ELEMENT_FILES=$(find "${WORKING_DIR}" \( -name 'a.bin' -o -name 'm%2Ekeys.bin' -o -name 'm%2Evalues.bin' \))
if [ "$(echo "${ELEMENT_FILES}" | wc -w)" -ne 3 ]
then
    echo "expected the three element files of table t under ${WORKING_DIR}, found: ${ELEMENT_FILES}" >&2
    exit 1
fi
SIZES_FILE=$(dirname "$(echo "${ELEMENT_FILES}" | head -n 1)")/sizes.json
if [ ! -f "${SIZES_FILE}" ]
then
    echo "${SIZES_FILE} does not exist" >&2
    exit 1
fi

for ELEMENT_FILE in ${ELEMENT_FILES}; do
    : > "${ELEMENT_FILE}"
done

# shellcheck disable=SC2086
python3 -c "
import json, os, sys, urllib.parse
sizes_path, *element_files = sys.argv[1:]
element_names = {os.path.basename(path) for path in element_files}
with open(sizes_path) as f:
    sizes = json.load(f)
for name, entry in sizes['clickhouse'].items():
    if urllib.parse.unquote(name) in element_names:
        entry['size'] = '0'
with open(sizes_path, 'w') as f:
    json.dump(sizes, f)
" "${SIZES_FILE}" ${ELEMENT_FILES}

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --query "SELECT a FROM t ORDER BY k" 2>&1 | grep -c -F 'INCORRECT_DATA'
${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --query "SELECT m FROM t ORDER BY k" 2>&1 | grep -c -F 'INCORRECT_DATA'

rm -rf "${WORKING_DIR}"
